"""
job_searcher.py
---------------
Automated job searcher — scrapes Indeed and LinkedIn, scores each job by weighted
keyword depth, filters by seniority/blacklists/keyword gate, and outputs a ranked list.

Maintains a persistent SQLite database (jobs.db) that accumulates every scrape run,
storing matched jobs AND rejected jobs with rejection reasons. Generates a static
GitHub Pages dashboard: index.html, analytics.html, settings.html.

All settings (country, site title, keywords, weights, blacklists, etc.) are configured
in config.json — editable from the browser via settings.html without touching code.

Setup:
    pip install python-jobspy pandas schedule

Usage:
    python job_searcher.py              # print to terminal
    python job_searcher.py --save       # also save to CSV
    python job_searcher.py --site       # also generate HTML dashboard
    python job_searcher.py --site --save  # full run (same as GitHub Actions)
    python job_searcher.py --daily      # run once now + schedule daily at 08:00
"""

import os
import re
import math
import hashlib
import sqlite3
import json
import pandas as pd
import schedule
import time
import argparse
from jobspy import scrape_jobs
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────
# CONFIG LOADING
# ─────────────────────────────────────────────────────────────────

# Output directory for generated files (index.html, CSV, jobs.db)
OUTPUT_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(OUTPUT_DIR, "jobs.db")
CONFIG_PATH = os.path.join(OUTPUT_DIR, "config.json")

_DEFAULT_CONFIG: dict = {
    "site_title": "My Job Search",
    "country":    "Netherlands",
    "search_queries": [
        "control systems engineer",
        "mechatronics engineer",
        "motion control engineer",
        "GNC engineer",
        "control engineer MATLAB",
        "systems and control engineer",
        "servo control engineer",
        "embedded control engineer",
        "flight dynamics engineer",
        "precision engineering control",
    ],
    "scored_keywords": [
        {
            "weight": 3,
            "keywords": [
                "mpc", "model predictive control",
                "robust control", "h-infinity", "h infinity", "mu-synthesis",
                "nonlinear control", "nonlinear systems",
                "kalman", "state observer", "luenberger",
                "lqr", "lqg",
                "state-space", "state space",
                "system identification", "sysid",
                "gnc", "guidance navigation control",
                "deep learning", "machine learning",
                "python", "pytorch", "tensorflow",
            ],
        },
        {
            "weight": 2,
            "keywords": [
                "matlab", "simulink",
                "control theory", "feedback control",
                "motion control", "servo", "servomotor",
                "mechatronics", "mechatronic",
                "robotics", "robot",
                "aerospace", "avionics", "flight control",
                "embedded control", "real-time control", "real time control",
            ],
        },
        {
            "weight": 1,
            "keywords": [
                "control", "automation", "dynamical systems",
                "high-tech", "high tech",
                "precision", "semiconductor",
                "sensor fusion", "localization",
                "navigation", "inertial",
                "signal processing",
                "pid", "pid control",
                "frequency domain", "bode", "nyquist",
                "optical", "lithography",
                "unmanned", "uav", "drone",
                "satellite", "space",
            ],
        },
    ],
    "blacklist_title": [
        "plc", "scada", "hmi",
        "automation engineer",
        "welding",
    ],
    "blacklist_full": [
        "ladder logic", "tia portal", "siemens s7", "step 7",
        "beckhoff ads", "codesys",
    ],
    "senior_patterns": [
        r"\bsenior\b", r"\bsr\b", r"\blead\b", r"\bprincipal\b",
        r"\bstaff\b", r"\bdirector\b", r"\bvp\b", r"\bhead of\b",
        r"\bmanager\b", r"\barchitect\b",
        r"\b[5-9]\+\s*year", r"\b1[0-9]\+\s*year",
    ],
    "target_companies": [
        "asml", "sioux", "nobleo", "demcon", "tmc", "nlr", "tno",
        "orange aerospace", "vanderlande", "lely",
        "thales", "fokker", "airbus", "canon production printing",
        "marin", "smart robotics", "avular", "philips",
        "nxp", "prodrive", "daf", "vi grade", "mecaer",
    ],
    "results_per_query": 30,
    "hours_old": 168,
    "min_keywords": 1,
}


def load_config() -> dict:
    """
    Load settings from config.json.
    If the file does not exist, write the default config and return it.
    If the file is malformed, warn and fall back to defaults.
    """
    if not os.path.exists(CONFIG_PATH):
        print(f"[config] config.json not found — writing defaults to {CONFIG_PATH}")
        _write_config(_DEFAULT_CONFIG)
        return _DEFAULT_CONFIG

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        print(f"[config] Loaded configuration from {CONFIG_PATH}")
        return cfg
    except Exception as exc:
        print(f"[config] Warning: could not parse config.json ({exc}) — using defaults.")
        return _DEFAULT_CONFIG


def _write_config(cfg: dict) -> None:
    """Write a config dict to config.json."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2, ensure_ascii=False)


# ─────────────────────────────────────────────────────────────────
# CONFIGURATION (loaded from config.json at startup)
# ─────────────────────────────────────────────────────────────────

_cfg = load_config()

SEARCH_QUERIES: list[str] = _cfg.get("search_queries", _DEFAULT_CONFIG["search_queries"])

# Support one location (old "country" field, kept for backward compatibility)
# OR several cities via a new "locations" list — e.g. ["Hangzhou, China", "Chengdu, China"].
# If "locations" is present in config.json it wins; otherwise falls back to "country".
LOCATIONS: list[str] = _cfg.get("locations") or [_cfg.get("country", _DEFAULT_CONFIG["country"])]
# Country string passed to Indeed's country_indeed filter (separate from city, since
# Indeed wants a country name, not a city). Defaults to "China" if not set.
COUNTRY_INDEED: str = _cfg.get("country_indeed", "China")
SITE_TITLE:   str = _cfg.get("site_title", _DEFAULT_CONFIG["site_title"])
RESULTS_PER_QUERY: int = int(_cfg.get("results_per_query", _DEFAULT_CONFIG["results_per_query"]))
HOURS_OLD:         int = int(_cfg.get("hours_old",         _DEFAULT_CONFIG["hours_old"]))
MIN_KEYWORDS:      int = max(0, int(_cfg.get("min_keywords", _DEFAULT_CONFIG["min_keywords"])))

# ── Scored keyword tiers ───────────────────────────────────────────
# Each keyword group has a weight. A job's score =
# sum over tiers of (weight × number of unique keywords matched in that tier).
# Theoretical max = _MAX_POSSIBLE_SCORE (every keyword in every tier matched).
# config.json stores these as [{weight: N, keywords: [...]}] objects.
SCORED_KEYWORDS: list[tuple[int, list[str]]] = [
    (int(entry["weight"]), list(entry["keywords"]))
    for entry in _cfg.get("scored_keywords", _DEFAULT_CONFIG["scored_keywords"])
]

# Flat list for the "required" gate — job must contain at least one weight-2/3 keyword
# (prevents pure weight-1 matches like "automation" in irrelevant jobs sneaking through)
GATE_KEYWORDS: list[str] = [kw for weight, group in SCORED_KEYWORDS if weight >= 2 for kw in group]

# ── Blacklist: title-scoped vs full-text ───────────────────────────
BLACKLIST_TITLE: list[str] = _cfg.get("blacklist_title", _DEFAULT_CONFIG["blacklist_title"])
BLACKLIST_FULL:  list[str] = _cfg.get("blacklist_full",  _DEFAULT_CONFIG["blacklist_full"])

# ── Seniority filter ───────────────────────────────────────────────
SENIOR_TITLE_PATTERNS: list[str] = _cfg.get("senior_patterns", _DEFAULT_CONFIG["senior_patterns"])

# Target companies — flagged with star and boosted in ranking
TARGET_COMPANIES: list[str] = _cfg.get("target_companies", _DEFAULT_CONFIG["target_companies"])

# Desired fit score — jobs at or above this score are highlighted as top matches.
# 0 = disabled (no highlighting).
DESIRED_SCORE: int = int(_cfg.get("desired_score", 0))


# ─────────────────────────────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────────────────────────────

def scrape_all() -> pd.DataFrame:
    """Scrape Indeed + LinkedIn for all search queries, across every configured city.
    Return combined, URL-deduped DataFrame."""
    all_frames: list[pd.DataFrame] = []

    for city in LOCATIONS:
        for query in SEARCH_QUERIES:
            print(f"  Searching: '{query}' in '{city}' ...")
            try:
                jobs = scrape_jobs(
                    site_name                  = ["indeed", "linkedin"],
                    search_term                = query,
                    location                   = city,
                    results_wanted             = RESULTS_PER_QUERY,
                    country_indeed             = COUNTRY_INDEED,
                    linkedin_fetch_description = True,
                    hours_old                  = HOURS_OLD,
                )
                if not jobs.empty:
                    jobs["query"] = query
                    jobs["search_city"] = city
                    all_frames.append(jobs)
                time.sleep(2)  # polite delay
            except Exception as exc:
                print(f"  [!] Error searching '{query}' in '{city}': {exc}")

    if not all_frames:
        print("  [!] No results returned from any query.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)

    # Primary dedup: exact URL
    if "job_url" in combined.columns:
        combined = combined.drop_duplicates(subset="job_url", keep="first")

    # Secondary dedup: same title + company (catches cross-board duplicates)
    combined = _fuzzy_dedup(combined)

    print(f"  {len(combined)} unique raw results after deduplication.")
    return combined


def _normalise_str(s: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation for fuzzy comparison."""
    s = str(s).lower().strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s)


def _fuzzy_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where (normalised title, normalised company) pair is duplicated."""
    if df.empty:
        return df
    df = df.copy()
    df["_title_norm"]   = df.get("title",   pd.Series(dtype=str)).fillna("").apply(_normalise_str)
    df["_company_norm"] = df.get("company", pd.Series(dtype=str)).fillna("").apply(_normalise_str)
    df["_dedup_key"]    = df["_title_norm"] + " || " + df["_company_norm"]
    before = len(df)
    df = df.drop_duplicates(subset="_dedup_key", keep="first")
    removed = before - len(df)
    if removed:
        print(f"  Fuzzy dedup removed {removed} cross-board duplicate(s).")
    df = df.drop(columns=["_title_norm", "_company_norm", "_dedup_key"])
    return df


# ─────────────────────────────────────────────────────────────────
# SCORING & FILTERING
# ─────────────────────────────────────────────────────────────────

_MAX_POSSIBLE_SCORE: float = sum(w * len(kws) for w, kws in SCORED_KEYWORDS)


def _compute_fit_score(text: str) -> tuple[float, list[str]]:
    """
    Score a job against SCORED_KEYWORDS. Returns (score, matched_kws).
    Each tier contributes weight * unique_keyword_count — so matching more
    keywords in a tier gives a proportionally higher score.
    """
    raw   = 0.0
    found: list[str] = []
    for weight, group in SCORED_KEYWORDS:
        hits = [kw for kw in group if kw in text]
        if hits:
            raw += weight * len(hits)   # multiply by count, not just binary
            found.extend(hits)  # all hits, highest-weight tiers first
    score = round(raw, 1)
    return score, found


def _count_matched_groups(text: str) -> int:
    """
    Count total unique individual keywords matched across ALL tiers.
    Used by the min_keywords gate — e.g. min_keywords=5 means at least
    5 individual keywords (across any tier) must appear in the job text.
    """
    return sum(
        1
        for _weight, group in SCORED_KEYWORDS
        for kw in group
        if kw in text
    )


def _is_senior_title(title: str) -> bool:
    """Return True if the title contains a seniority signal Oscar cannot pass."""
    t = title.lower()
    return any(re.search(p, t) for p in SENIOR_TITLE_PATTERNS)


def _passes_blacklist(title_text: str, full_text: str) -> bool:
    """Return True if the job is NOT blacklisted."""
    title_lower = title_text.lower()
    full_lower  = full_text.lower()
    if any(kw in title_lower for kw in BLACKLIST_TITLE):
        return False
    if any(kw in full_lower for kw in BLACKLIST_FULL):
        return False
    return True


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Apply seniority filter, blacklist, gate, and fit scoring. Return ranked DataFrame."""
    if df.empty:
        return df

    df = df.copy()

    # Build searchable text blobs
    text_cols = ["title", "company", "description", "location"]
    available = [c for c in text_cols if c in df.columns]
    df["_full_text"]  = df[available].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
    df["_title_text"] = df.get("title", pd.Series(dtype=str)).fillna("").str.lower()

    # 1. Seniority filter (title only)
    before = len(df)
    df = df[~df["_title_text"].apply(_is_senior_title)]
    print(f"  Seniority filter removed {before - len(df)} senior/lead roles.")

    # 2. Blacklist
    before = len(df)
    df = df[df.apply(lambda r: _passes_blacklist(r["_title_text"], r["_full_text"]), axis=1)]
    print(f"  Blacklist removed {before - len(df)} jobs.")

    # 3. Gate: must match at least one weight-2 or weight-3 keyword
    gate_pattern = "|".join(re.escape(kw) for kw in GATE_KEYWORDS)
    before = len(df)
    df = df[df["_full_text"].str.contains(gate_pattern, regex=True)]
    print(f"  Gate filter removed {before - len(df)} low-signal jobs.")

    # 3b. Minimum total keywords gate (configurable via min_keywords)
    if MIN_KEYWORDS > 1 and not df.empty:
        before = len(df)
        df = df[df["_full_text"].apply(_count_matched_groups) >= MIN_KEYWORDS]
        print(f"  Min-keywords gate (>= {MIN_KEYWORDS} total keywords) removed {before - len(df)} jobs.")

    if df.empty:
        print("  No jobs passed all filters.")
        return df

    # 4. Fit score
    scores_and_kws = df["_full_text"].apply(_compute_fit_score)
    df["fit_score"]       = scores_and_kws.apply(lambda x: x[0])
    df["matched_keywords"] = scores_and_kws.apply(lambda x: x[1])

    # 5. Target company flag
    df["is_target"] = df.get("company", pd.Series(dtype=str)).fillna("").str.lower().apply(
        lambda c: any(tc in c for tc in TARGET_COMPANIES)
    )

    # 6. Sort: target companies first, then by fit score descending
    df = df.sort_values(
        by=["is_target", "fit_score"],
        ascending=[False, False]
    )

    # Drop internal helper columns
    df = df.drop(columns=["_full_text", "_title_text"])

    print(f"  {len(df)} jobs passed all filters.")
    return df


# ─────────────────────────────────────────────────────────────────
# CATEGORY CLASSIFICATION
# ─────────────────────────────────────────────────────────────────

# Category definitions: evaluated in order, first match wins.
# Each entry: (category_name, list_of_keyword_signals)
_CATEGORY_RULES: list[tuple[str, list[str]]] = [
    ("GNC/Aerospace", [
        "gnc", "guidance navigation control", "flight control", "flight dynamics",
        "avionics", "aerospace", "satellite", "space systems", "astrodynamics",
        "uav", "drone", "unmanned", "fokker", "airbus", "nlr", "orange aerospace",
    ]),
    ("High-Tech/Precision", [
        "asml", "lithography", "semiconductor", "wafer", "optical",
        "high-tech", "high tech", "precision motion", "canon production printing",
        "nxp", "philips", "prodrive", "demcon", "sioux",
    ]),
    ("Robotics", [
        "robotics", "robot", "ros", "ros2", "manipulator", "mobile robot",
        "autonomous", "smart robotics", "avular", "nobleo",
    ]),
    ("General Control", [
        "control systems", "control engineer", "mechatronics", "motion control",
        "servo", "embedded control", "state-space", "matlab", "simulink",
        "feedback control", "pid control",
    ]),
]

_BLACKLISTED_CATEGORY_RULES: list[tuple[str, list[str]]] = [
    ("Blacklisted-PLC", [
        "plc", "scada", "hmi", "ladder logic", "tia portal",
        "siemens s7", "step 7", "beckhoff ads", "codesys",
    ]),
    ("Blacklisted-Other", []),  # catch-all for other blacklisted reasons
]


def _classify_job(title: str, full_text: str, rejection_reason: str) -> str:
    """
    Assign a category string to a job.
    Accepted jobs are classified by domain. Rejected jobs get a Blacklisted/Rejected label.
    """
    if rejection_reason:
        if "senior" in rejection_reason.lower() or "seniority" in rejection_reason.lower():
            return "Rejected-Senior"
        title_lower = title.lower()
        full_lower  = full_text.lower()
        plc_signals = [
            "plc", "scada", "hmi", "automation engineer", "welding",
            "ladder logic", "tia portal", "siemens s7", "step 7",
            "beckhoff ads", "codesys",
        ]
        if any(s in title_lower or s in full_lower for s in plc_signals):
            return "Blacklisted-PLC"
        if "gate" in rejection_reason.lower() or "no gate" in rejection_reason.lower():
            return "Rejected-No-Gate"
        return "Blacklisted-Other"

    # Accepted job — classify by domain
    combined = (title + " " + full_text).lower()
    for category, signals in _CATEGORY_RULES:
        if any(s in combined for s in signals):
            return category

    return "General Control"  # fallback for accepted jobs


# ─────────────────────────────────────────────────────────────────
# SQLITE PERSISTENCE
# ─────────────────────────────────────────────────────────────────

def _init_db() -> sqlite3.Connection:
    """
    Open (or create) jobs.db and ensure the schema exists.
    Returns an open connection. Caller is responsible for closing it.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id           TEXT NOT NULL,
            run_date         TEXT NOT NULL,
            year             INTEGER NOT NULL,
            week_number      INTEGER NOT NULL,
            title            TEXT,
            company          TEXT,
            location         TEXT,
            site             TEXT,
            fit_score        REAL,
            category         TEXT,
            is_target        INTEGER,
            job_url          TEXT,
            rejection_reason TEXT,
            date_posted      TEXT,
            matched_keywords TEXT,
            PRIMARY KEY (job_id, run_date)
        )
    """)
    # Migrate existing DBs that predate these columns
    for _col, _type in [("date_posted", "TEXT"), ("matched_keywords", "TEXT"), ("description", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {_col} {_type}")
        except Exception:
            pass  # Column already exists — ignore
    conn.commit()
    return conn


def _job_id(title: str, company: str, url: str) -> str:
    """
    Stable hash-based ID for a job. Uses URL when available; falls back to
    title+company. This lets the DB track whether the same posting reappears
    across multiple scrape runs without creating duplicate primary keys.
    """
    key = url.strip() if url and url != "#" else f"{title}|{company}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def save_to_db(
    accepted_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    accepted_ids: set,
    run_dt: datetime,
):
    """
    Persist this scrape run to jobs.db.

    Accepted jobs: all rows in accepted_df.
    Rejected jobs: all rows in raw_df whose job_id is NOT in accepted_ids,
                   with a computed rejection_reason.
    """
    conn = _init_db()
    run_date    = run_dt.strftime("%Y-%m-%d")
    year        = run_dt.isocalendar()[0]
    week_number = run_dt.isocalendar()[1]

    rows_inserted = 0
    rows_skipped  = 0

    def _upsert(job_id, title, company, location, site, fit_score,
                category, is_target, job_url, rejection_reason,
                date_posted=None, matched_keywords=None, description=None):
        nonlocal rows_inserted, rows_skipped
        mk_json = json.dumps(matched_keywords) if matched_keywords else "[]"
        desc_truncated = (description or "")[:500] if description else None
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO jobs
                  (job_id, run_date, year, week_number,
                   title, company, location, site,
                   fit_score, category, is_target, job_url, rejection_reason,
                   date_posted, matched_keywords, description)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    job_id, run_date, year, week_number,
                    title, company, location, site,
                    fit_score, category, int(bool(is_target)),
                    job_url, rejection_reason, date_posted, mk_json, desc_truncated,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                rows_inserted += 1
            else:
                rows_skipped += 1
            # Backfill date_posted, description and matched_keywords on ALL
            # existing rows for this job_id where they are still missing
            # (inserted before these columns existed).
            # Safe to run every time — only touches NULL / empty values.
            if desc_truncated or (mk_json and mk_json != "[]") or date_posted:
                conn.execute(
                    """
                    UPDATE jobs SET
                        date_posted      = COALESCE(date_posted,                  ?),
                        description      = COALESCE(NULLIF(description, ''),      ?),
                        matched_keywords = COALESCE(NULLIF(matched_keywords, ''), NULLIF(?, '[]'), matched_keywords)
                    WHERE job_id = ?
                      AND (date_posted IS NULL
                           OR description IS NULL OR description = ''
                           OR matched_keywords IS NULL OR matched_keywords = '[]')
                    """,
                    (date_posted, desc_truncated, mk_json, job_id),
                )
        except Exception as exc:
            print(f"  [DB] Insert error for '{title}': {exc}")

    # ── Accepted jobs ──────────────────────────────────────────────
    for _, row in accepted_df.iterrows():
        title   = str(row.get("title",   "") or "")
        company = str(row.get("company", "") or "")
        url     = str(row.get("job_url", "") or "")
        jid     = _job_id(title, company, url)
        full_text = (
            title + " " +
            str(row.get("description", "") or "")
        ).lower()
        category = _classify_job(title, full_text, rejection_reason="")
        # Normalise date_posted to a YYYY-MM-DD string (or None)
        _dp = row.get("date_posted") or row.get("date")
        if _dp is not None and not (isinstance(_dp, float) and pd.isna(_dp)):
            try:
                date_posted_str = pd.to_datetime(_dp).strftime("%Y-%m-%d")
            except Exception:
                date_posted_str = None
        else:
            date_posted_str = None
        _upsert(
            job_id           = jid,
            title            = title,
            company          = company,
            location         = str(row.get("location", "") or ""),
            site             = str(row.get("site", "") or ""),
            fit_score        = float(row.get("fit_score", 0) or 0),
            category         = category,
            is_target        = bool(row.get("is_target", False)),
            job_url          = url,
            rejection_reason = None,
            date_posted      = date_posted_str,
            matched_keywords = list(row.get("matched_keywords") or []),
            description      = str(row.get("description", "") or "")[:500],
        )

    # ── Rejected jobs ──────────────────────────────────────────────
    # Rebuild full_text for raw_df rows the same way filter_jobs does.
    if not raw_df.empty:
        text_cols = ["title", "company", "description", "location"]
        available = [c for c in text_cols if c in raw_df.columns]
        raw_df = raw_df.copy()
        raw_df["_full_text"]  = raw_df[available].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        raw_df["_title_text"] = raw_df.get("title", pd.Series(dtype=str)).fillna("").str.lower()

        gate_pattern = "|".join(re.escape(kw) for kw in GATE_KEYWORDS)

        for _, row in raw_df.iterrows():
            title     = str(row.get("title",   "") or "")
            company   = str(row.get("company", "") or "")
            url       = str(row.get("job_url", "") or "")
            jid       = _job_id(title, company, url)

            if jid in accepted_ids:
                continue  # already recorded above

            title_lower = row["_title_text"]
            full_lower  = row["_full_text"]

            # Determine rejection reason
            if _is_senior_title(title_lower):
                reason = "Seniority filter: title contains senior/lead/principal"
            elif not _passes_blacklist(title_lower, full_lower):
                # Determine which blacklist triggered
                plc_title = any(kw in title_lower for kw in BLACKLIST_TITLE)
                reason = (
                    "Blacklisted (title): PLC/SCADA/HMI/automation engineer/welding"
                    if plc_title
                    else "Blacklisted (full text): ladder logic/TIA Portal/Beckhoff/CodeSys"
                )
            elif not re.search(gate_pattern, full_lower):
                reason = "No gate keyword: missing weight-2/3 signal (MATLAB/robotics/control etc.)"
            elif MIN_KEYWORDS > 1 and _count_matched_groups(full_lower) < MIN_KEYWORDS:
                reason = f"Below min_keywords gate: fewer than {MIN_KEYWORDS} total keywords matched across all tiers"
            else:
                reason = "Below fit threshold or unclassified rejection"

            category = _classify_job(title, full_lower, rejection_reason=reason)
            _upsert(
                job_id           = jid,
                title            = title,
                company          = company,
                location         = str(row.get("location", "") or ""),
                site             = str(row.get("site", "") or ""),
                fit_score        = 0.0,
                category         = category,
                is_target        = False,
                job_url          = url,
                rejection_reason = reason,
            )

    conn.commit()
    conn.close()
    print(f"  [DB] {rows_inserted} rows inserted, {rows_skipped} already present — {DB_PATH}")


# ─────────────────────────────────────────────────────────────────
# ANALYTICS DATA PREPARATION
# ─────────────────────────────────────────────────────────────────

def _aggregate_records(
    records: list[dict],
    period_keys: list[str],
    granularity: str = "week",
) -> dict:
    """
    Aggregate a list of job records into the chart payload shape.

    granularity: "week" (default) groups by ISO week string "YYYY-Www";
                 "day" groups by run_date string "YYYY-MM-DD".

    Records passed in must already be deduplicated by job_id (first-seen only).
    """
    all_categories = [
        "GNC/Aerospace", "High-Tech/Precision", "Robotics", "General Control",
        "Blacklisted-PLC", "Blacklisted-Other", "Rejected-Senior", "Rejected-No-Gate",
    ]

    def period_key(r: dict) -> str:
        if granularity == "day":
            return r["run_date"]
        return f"{r['year']}-W{r['week_number']:02d}"

    accepted = [r for r in records if not r["rejection_reason"]]
    rejected = [r for r in records if r["rejection_reason"]]

    jobs_per_period     = {pk: 0 for pk in period_keys}
    rejected_per_period = {pk: 0 for pk in period_keys}
    for r in accepted:
        pk = period_key(r)
        if pk in jobs_per_period:
            jobs_per_period[pk] += 1
    for r in rejected:
        pk = period_key(r)
        if pk in rejected_per_period:
            rejected_per_period[pk] += 1

    cat_per_period = {pk: {cat: 0 for cat in all_categories} for pk in period_keys}
    for r in records:
        pk  = period_key(r)
        cat = r["category"] or "General Control"
        if pk in cat_per_period and cat in cat_per_period[pk]:
            cat_per_period[pk][cat] += 1

    sources = ["linkedin", "indeed", "other"]
    src_per_period = {pk: {s: 0 for s in sources} for pk in period_keys}
    for r in accepted:
        pk  = period_key(r)
        src = (r["site"] or "other").lower()
        if src not in sources:
            src = "other"
        if pk in src_per_period:
            src_per_period[pk][src] += 1

    company_counts: dict[str, int] = {}
    for r in accepted:
        c = (r["company"] or "Unknown").strip()
        company_counts[c] = company_counts.get(c, 0) + 1
    top_companies = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    ratio_per_period = []
    for pk in period_keys:
        acc = jobs_per_period[pk]
        rej = rejected_per_period[pk]
        total = acc + rej
        ratio_per_period.append({
            "week":     pk,
            "accepted": acc,
            "rejected": rej,
            "total":    total,
            "accept_pct": round(acc / total * 100, 1) if total else 0,
        })

    period_accepted_list = [jobs_per_period[pk] for pk in period_keys]
    rolling_avg = []
    for i in range(len(period_accepted_list)):
        window = period_accepted_list[max(0, i - 3): i + 1]
        rolling_avg.append(round(sum(window) / len(window), 2))

    fit_sum   = {pk: 0.0 for pk in period_keys}
    fit_count = {pk: 0   for pk in period_keys}
    for r in accepted:
        pk = period_key(r)
        if pk in fit_sum:
            fit_sum[pk]   += r["fit_score"] or 0
            fit_count[pk] += 1
    avg_fit_per_period = [
        round(fit_sum[pk] / fit_count[pk], 2) if fit_count[pk] else 0
        for pk in period_keys
    ]

    target_per_period = {pk: 0 for pk in period_keys}
    for r in accepted:
        if r["is_target"]:
            pk = period_key(r)
            if pk in target_per_period:
                target_per_period[pk] += 1

    desired_per_period = {pk: 0 for pk in period_keys}
    if DESIRED_SCORE > 0:
        for r in accepted:
            if (r["fit_score"] or 0) >= DESIRED_SCORE:
                pk = period_key(r)
                if pk in desired_per_period:
                    desired_per_period[pk] += 1

    total_accept = len(accepted)
    total_reject = len(rejected)
    avg_fit_all = (
        round(sum(r["fit_score"] or 0 for r in accepted) / total_accept, 2)
        if total_accept else 0
    )
    total_desired = sum(desired_per_period.values())

    return {
        "jobs_per_week":     [jobs_per_period[pk] for pk in period_keys],
        "rejected_per_week": [rejected_per_period[pk] for pk in period_keys],
        "rolling_avg":       rolling_avg,
        "avg_fit_per_week":  avg_fit_per_period,
        "target_per_week":   [target_per_period[pk] for pk in period_keys],
        "desired_per_week":  [desired_per_period[pk] for pk in period_keys],
        "cat_per_week":      {cat: [cat_per_period[pk][cat] for pk in period_keys] for cat in all_categories},
        "src_per_week":      {src: [src_per_period[pk][src] for pk in period_keys] for src in sources},
        "top_companies":     [{"company": c, "count": n} for c, n in top_companies],
        "ratio_per_week":    ratio_per_period,
        "summary": {
            "total_seen":     len(records),
            "total_accepted": total_accept,
            "total_rejected": total_reject,
            "avg_fit":        avg_fit_all,
            "total_desired":  total_desired,
        },
    }


def _load_analytics_data() -> dict:
    """
    Read jobs.db and produce a dict of pre-aggregated analytics payloads
    that will be embedded as JSON into analytics.html.
    Returns an empty-safe dict even if the DB does not yet exist.

    Deduplication: each job_id is counted only once, using the row from its
    earliest run_date.  This prevents jobs that re-appear across multiple
    scrape runs from inflating weekly/daily totals.

    Two granularities are returned:
      all_weekly — grouped by ISO week  ("YYYY-Www")
      all_daily  — grouped by calendar day ("YYYY-MM-DD"), last 90 days
    """
    if not os.path.exists(DB_PATH):
        return {}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM jobs ORDER BY run_date, year, week_number"
    ).fetchall()
    conn.close()

    if not rows:
        return {}

    all_rows = [dict(r) for r in rows]

    # ── Step 1: deduplicate by job_id, keeping the first-seen row ────
    seen_jids: set[str] = set()
    records: list[dict] = []
    # Rows are already ordered by run_date ASC so the first encounter is earliest.
    for r in all_rows:
        jid = r["job_id"]
        if jid not in seen_jids:
            seen_jids.add(jid)
            records.append(r)

    # Build first_seen_date map from the deduplicated records (run_date IS first-seen here).
    first_seen_date: dict[str, str] = {r["job_id"]: r["run_date"] for r in records}

    total_runs = len(set(r["run_date"] for r in all_rows))
    last_run   = max(r["run_date"] for r in all_rows)

    # ── Step 2: weekly keys ───────────────────────────────────────────
    week_keys: list[str] = sorted(
        set(f"{r['year']}-W{r['week_number']:02d}" for r in records)
    )

    # ── Step 3: daily keys — last 90 calendar days that have any data ─
    # Include every date in the continuous range so the chart axis is uniform.
    if records:
        min_date = datetime.strptime(min(r["run_date"] for r in records), "%Y-%m-%d")
        max_date = datetime.strptime(max(r["run_date"] for r in records), "%Y-%m-%d")
        cutoff   = max_date - timedelta(days=89)          # 90-day window inclusive
        start    = max(min_date, cutoff)
        day_keys: list[str] = []
        cur = start
        while cur <= max_date:
            day_keys.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
    else:
        day_keys = []

    # ── Step 4: aggregate for both granularities ──────────────────────
    full_weekly = _aggregate_records(records, week_keys, granularity="week")
    full_daily  = _aggregate_records(records, day_keys,  granularity="day")

    for full in (full_weekly, full_daily):
        full["summary"]["total_runs"] = total_runs
        full["summary"]["last_run"]   = last_run

    # ── Step 5: new_jobs series for both granularities ────────────────
    # For weekly: first-seen week of each accepted job.
    def _new_jobs_weekly() -> list[int]:
        counts: dict[str, int] = {wk: 0 for wk in week_keys}
        for r in records:
            if r["rejection_reason"]:
                continue
            fs_date = r["run_date"]               # deduplicated, so this IS first-seen
            parsed  = datetime.strptime(fs_date, "%Y-%m-%d")
            iso     = parsed.isocalendar()
            fsw     = f"{iso[0]}-W{iso[1]:02d}"
            if fsw in counts:
                counts[fsw] += 1
        return [counts[wk] for wk in week_keys]

    # For daily: first-seen date of each accepted job (only if within day_keys range).
    def _new_jobs_daily() -> list[int]:
        day_set = set(day_keys)
        counts: dict[str, int] = {dk: 0 for dk in day_keys}
        for r in records:
            if r["rejection_reason"]:
                continue
            dk = r["run_date"]
            if dk in day_set:
                counts[dk] += 1
        return [counts[dk] for dk in day_keys]

    full_weekly["new_jobs_per_week"] = _new_jobs_weekly()
    full_daily["new_jobs_per_week"]  = _new_jobs_daily()

    # ── Step 6: lightweight per-job records for client-side filtering ─
    # One entry per job_id (deduplicated).  Includes both wk and fsd (first-seen date)
    # so the JS _rebuildViewFromRecords can work in both granularities.
    job_records = []
    for r in records:
        if r["rejection_reason"]:
            continue
        fs_date = r["run_date"]
        parsed  = datetime.strptime(fs_date, "%Y-%m-%d")
        iso     = parsed.isocalendar()
        fsw     = f"{iso[0]}-W{iso[1]:02d}"
        sc      = round(r["fit_score"] or 0, 1)
        job_records.append({
            "wk":  f"{r['year']}-W{r['week_number']:02d}",
            "sc":  sc,
            "cat": r["category"] or "General Control",
            "tgt": 1 if r["is_target"] else 0,
            "src": (r["site"] or "other").lower(),
            "co":  (r["company"] or "Unknown").strip(),
            "fsw": fsw,
            "fsd": fs_date,          # first-seen YYYY-MM-DD for daily mode
            "ds":  1 if (DESIRED_SCORE > 0 and sc >= DESIRED_SCORE) else 0,
        })

    return {
        "week_labels":    week_keys,
        "day_labels":     day_keys,
        "all_weekly":     full_weekly,
        "all_daily":      full_daily,
        # Keep "all" as an alias pointing to weekly so existing JS references
        # (e.g. inside _rebuildViewFromRecords) continue to work.
        "all":            full_weekly,
        "job_records":    job_records,
        "desired_score":  DESIRED_SCORE,
    }


# ─────────────────────────────────────────────────────────────────
# ANALYTICS HTML GENERATOR
# ─────────────────────────────────────────────────────────────────

# Colours for the category stacked bar — must stay consistent with all_categories order.
_CATEGORY_COLORS = {
    "GNC/Aerospace":       "#1a3a5c",
    "High-Tech/Precision": "#2e86ab",
    "Robotics":            "#28a745",
    "General Control":     "#6c757d",
    "Blacklisted-PLC":     "#dc3545",
    "Blacklisted-Other":   "#fd7e14",
    "Rejected-Senior":     "#ffc107",
    "Rejected-No-Gate":    "#adb5bd",
}

_SOURCE_COLORS = {
    "linkedin": "#0a66c2",
    "indeed":   "#2e7d32",
    "other":    "#888888",
}


def generate_analytics():
    """
    Read jobs.db, build analytics payload, and write analytics.html.
    The page is fully static — all data is embedded as JSON so it works
    on GitHub Pages without any server-side rendering.
    """
    data = _load_analytics_data()
    data_json = json.dumps(data, ensure_ascii=False, indent=None)

    updated = datetime.now().strftime("%d %B %Y at %H:%M UTC")

    # Category and source colour maps — passed into the JS as JSON so the
    # client can build datasets dynamically after range-slicing.
    cat_colors_json = json.dumps(_CATEGORY_COLORS)
    src_colors_json = json.dumps(_SOURCE_COLORS)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Analytics — {SITE_TITLE}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #f0f2f5; color: #2c3e50;
}}

header {{
  background: #1a3a5c; color: white; padding: 22px 32px;
  display: flex; justify-content: space-between; align-items: center;
  flex-wrap: wrap; gap: 10px;
}}
header h1 {{ font-size: 20px; font-weight: 600; }}
header p  {{ font-size: 12px; opacity: 0.65; margin-top: 4px; }}
.nav-link {{
  color: rgba(255,255,255,0.85); font-size: 13px; font-weight: 500;
  text-decoration: none; border: 1px solid rgba(255,255,255,0.4);
  padding: 6px 14px; border-radius: 6px;
}}
.nav-link:hover {{ background: rgba(255,255,255,0.15); }}

/* ── Stat cards ──────────────────────────────────────────────── */
.stat-cards {{
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
  padding: 20px 32px;
  background: white;
  border-bottom: 1px solid #e0e0e0;
}}
.stat-card {{
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  padding: 16px 20px;
  display: flex;
  flex-direction: column;
  gap: 4px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}}
.stat-card .stat-value {{
  font-size: 28px; font-weight: 700; color: #1a3a5c; line-height: 1.1;
}}
.stat-card .stat-label {{
  font-size: 12px; color: #64748b; font-weight: 500; text-transform: uppercase;
  letter-spacing: 0.04em;
}}
.stat-card .stat-trend {{
  font-size: 11px; margin-top: 4px; font-weight: 600;
}}
.stat-trend.up   {{ color: #16a34a; }}
.stat-trend.down {{ color: #dc2626; }}
.stat-trend.flat {{ color: #94a3b8; }}

/* ── Range selector strip ────────────────────────────────────── */
.range-strip {{
  background: #f8fafc;
  border-bottom: 1px solid #e0e0e0;
  padding: 12px 32px;
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}}
.range-strip label {{
  font-size: 12px; font-weight: 600; color: #475569; white-space: nowrap;
}}
.range-strip select {{
  font-size: 12px; padding: 5px 10px; border-radius: 6px;
  border: 1px solid #cbd5e1; background: white; color: #334155;
  cursor: pointer;
}}
.range-strip select:focus {{ outline: 2px solid #2e86ab; outline-offset: 1px; }}
.range-quick {{
  display: flex; gap: 6px; margin-left: 8px; flex-wrap: wrap;
}}
.range-btn {{
  font-size: 12px; font-weight: 500; padding: 5px 12px;
  border-radius: 6px; border: 1px solid #cbd5e1;
  background: white; color: #475569; cursor: pointer;
  transition: background 0.15s, color 0.15s, border-color 0.15s;
}}
.range-btn:hover {{ background: #e2e8f0; }}
.range-btn.active {{
  background: #1a3a5c; color: white; border-color: #1a3a5c;
}}

/* ── Granularity toggle (Daily / Weekly) ─────────────────────── */
.gran-toggle {{
  background: #f0f2f5; border-bottom: 1px solid #e0e0e0;
  padding: 10px 32px; display: flex; gap: 8px; align-items: center;
  flex-wrap: wrap;
}}
.gran-btn {{
  font-size: 12px; font-weight: 600; padding: 5px 16px;
  border-radius: 6px; border: 1px solid #cbd5e1;
  background: white; color: #475569; cursor: pointer;
  transition: background 0.15s, color 0.15s, border-color 0.15s;
}}
.gran-btn:hover {{ background: #e2e8f0; }}
.gran-btn.active {{
  background: #1a3a5c; color: white; border-color: #1a3a5c;
}}
.gran-label {{
  font-size: 12px; font-weight: 600; color: #475569; margin-right: 4px;
}}

/* ── View toggle ─────────────────────────────────────────────── */
.view-toggle {{
  background: white; border-bottom: 1px solid #e0e0e0;
  padding: 12px 32px; display: flex; gap: 14px; align-items: center;
  font-size: 13px; flex-wrap: wrap;
}}
.view-toggle label {{
  display: inline-flex; align-items: center; gap: 8px;
  cursor: pointer; user-select: none; font-weight: 500; color: #444;
}}

/* ── Layout ──────────────────────────────────────────────────── */
.section {{
  padding: 24px 32px;
}}
.section-title {{
  font-size: 15px; font-weight: 600; color: #1a3a5c;
  margin-bottom: 16px; border-left: 3px solid #2e86ab;
  padding-left: 10px;
}}

.charts-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(520px, 1fr));
  gap: 20px;
}}

.chart-card {{
  background: white; border-radius: 10px; padding: 20px 22px;
  border: 1px solid #e8e8e8;
}}
.chart-card h3 {{
  font-size: 13px; font-weight: 600; color: #555;
  margin-bottom: 14px;
}}
.chart-wrap {{
  position: relative; height: 260px;
}}
.chart-wide {{ grid-column: 1 / -1; }}
.chart-wide .chart-wrap {{ height: 280px; }}

/* ── Tables ──────────────────────────────────────────────────── */
table {{
  width: 100%; border-collapse: collapse; font-size: 13px;
}}
th {{
  background: #f8f9fa; text-align: left; padding: 8px 12px;
  font-weight: 600; color: #555; border-bottom: 2px solid #e8e8e8;
}}
th.sortable {{
  cursor: pointer; user-select: none; white-space: nowrap;
}}
th.sortable:hover {{ background: #eef0f2; color: #1a3a5c; }}
th.sort-asc::after  {{ content: ' ▲'; font-size: 10px; opacity: 0.7; }}
th.sort-desc::after {{ content: ' ▼'; font-size: 10px; opacity: 0.7; }}
td {{
  padding: 8px 12px; border-bottom: 1px solid #f0f0f0; color: #333;
}}
tr:hover td {{ background: #fafafa; }}

.bar-cell {{ display: flex; align-items: center; gap: 8px; }}
.mini-bar {{
  height: 8px; background: #e8e8e8; border-radius: 4px;
  flex: 1; min-width: 60px; max-width: 160px; overflow: hidden;
}}
.mini-bar-fill {{
  height: 100%; background: #1a3a5c; border-radius: 4px;
}}

.trend-badge {{
  display: inline-block; padding: 3px 9px; border-radius: 10px;
  font-size: 11px; font-weight: 600;
}}
.trend-up   {{ background: #d4edda; color: #155724; }}
.trend-down {{ background: #f8d7da; color: #721c24; }}
.trend-flat {{ background: #e2e3e5; color: #495057; }}

.no-data {{
  text-align: center; padding: 80px 0; color: #aaa; font-size: 15px;
}}

footer {{
  text-align: center; padding: 24px; font-size: 11px; color: #bbb;
}}

@media (max-width: 700px) {{
  .charts-grid {{ grid-template-columns: 1fr; }}
  .stat-cards  {{ grid-template-columns: repeat(2, 1fr); padding: 14px 16px; }}
  .section     {{ padding: 16px; }}
  header, .range-strip, .gran-toggle, .view-toggle {{ padding-left: 16px; padding-right: 16px; }}
  .range-quick {{ margin-left: 0; }}
}}
@media (max-width: 480px) {{
  .stat-cards {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>

<header>
  <div>
    <h1>Analytics — {SITE_TITLE}</h1>
    <p>Updated: {updated} &nbsp;|&nbsp; Data from jobs.db — all scrape runs accumulated</p>
  </div>
  <div style="display:flex; gap:8px; flex-wrap:wrap;">
    <a class="nav-link" href="index.html">Live Job Board</a>
    <a class="nav-link" href="settings.html">&#9881; Settings</a>
  </div>
</header>

<!-- Stat cards — populated by JS -->
<div class="stat-cards" id="statCards">
  <div class="stat-card" style="grid-column:1/-1;">
    <div class="stat-label">Status</div>
    <div class="stat-value" style="font-size:16px;color:#aaa;">No data yet — run job_searcher.py --site to populate.</div>
  </div>
</div>

<!-- Granularity toggle: Daily / Weekly -->
<div class="gran-toggle" id="granToggle" style="display:none;">
  <span class="gran-label">View:</span>
  <button class="gran-btn active" id="btnWeekly" onclick="setGranularity('week', this)">Weekly</button>
  <button class="gran-btn"        id="btnDaily"  onclick="setGranularity('day',  this)">Daily</button>
</div>

<!-- Range selector strip -->
<div class="range-strip" id="rangeStrip" style="display:none;">
  <label for="fromPeriod" id="fromPeriodLabel">From week</label>
  <select id="fromPeriod" onchange="onRangeDropdown()"></select>
  <label for="toPeriod" id="toPeriodLabel">To week</label>
  <select id="toPeriod" onchange="onRangeDropdown()"></select>
  <div class="range-quick" id="rangeQuickBtns">
    <button class="range-btn" onclick="setQuickRange(4,  this)">Last 4</button>
    <button class="range-btn" onclick="setQuickRange(8,  this)">Last 8</button>
    <button class="range-btn" onclick="setQuickRange(12, this)">Last 12</button>
    <button class="range-btn" onclick="setQuickRange(30, this)">Last 30</button>
    <button class="range-btn" onclick="setQuickRange(0,  this)">All time</button>
  </div>
</div>

<div class="view-toggle">
  <span style="display:inline-flex; align-items:center; gap:8px;">
    <label for="minScoreFilter" style="font-weight:500; color:#444; white-space:nowrap;">Min score:</label>
    <input type="number" id="minScoreFilter" min="0" step="1" placeholder="any"
           oninput="onScoreFilter()"
           style="width:72px; padding:5px 8px; border:1px solid #ccc; border-radius:6px;
                  font-size:13px; outline:none;">
  </span>
  <span style="display:inline-flex; align-items:center; gap:8px; margin-left:16px;">
    <label style="font-weight:500; color:#444; white-space:nowrap;">New jobs score &ge;:</label>
    <input type="number" id="newJobScoreFilter" min="0" step="1" placeholder="any"
           oninput="onNewJobScoreFilter()"
           style="width:72px; padding:5px 8px; border:1px solid #ccc; border-radius:6px; font-size:13px;">
  </span>
</div>

<div class="section">
  <div class="section-title">Market Volume Over Time</div>
  <div class="charts-grid">

    <div class="chart-card chart-wide">
      <h3 id="titleChartWeekly">Jobs found per period (matched vs rejected)</h3>
      <div class="chart-wrap">
        <canvas id="chartWeekly"></canvas>
      </div>
    </div>

    <div class="chart-card chart-wide">
      <h3 id="titleChartNewJobs">New jobs discovered per period (first appearance)</h3>
      <div class="chart-wrap">
        <canvas id="chartNewJobs"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3 id="titleChartSource">Source: LinkedIn vs Indeed per period</h3>
      <div class="chart-wrap">
        <canvas id="chartSource"></canvas>
      </div>
    </div>

  </div>
</div>

<div class="section">
  <div class="section-title">Quality &amp; Trend Signals</div>
  <div class="charts-grid">

    <div class="chart-card">
      <h3 id="titleChartTrend">Rolling 4-period average (matched jobs) — is the market improving?</h3>
      <div class="chart-wrap">
        <canvas id="chartTrend"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3 id="titleChartFit">Average fit score per period</h3>
      <div class="chart-wrap">
        <canvas id="chartFit"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3 id="titleChartTarget">Target company appearances per period</h3>
      <div class="chart-wrap">
        <canvas id="chartTarget"></canvas>
      </div>
    </div>

    <div class="chart-card" id="chartDesiredCard" style="display:none;">
      <h3 id="titleChartDesired">Desired matches per period (score &ge; desired threshold)</h3>
      <div class="chart-wrap">
        <canvas id="chartDesired"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3 id="titleChartRatio">Match rate: accepted / (accepted + rejected) per period</h3>
      <div class="chart-wrap">
        <canvas id="chartRatio"></canvas>
      </div>
    </div>

  </div>
</div>

<div class="section">
  <div class="section-title">Top Companies in Results</div>
  <div class="charts-grid" style="margin-bottom:16px;">
    <div class="chart-card chart-wide">
      <h3>Top 10 companies by appearance count</h3>
      <div class="chart-wrap" style="height:320px;">
        <canvas id="chartCompanies"></canvas>
      </div>
    </div>
  </div>
  <div style="background:white; border-radius:10px; border:1px solid #e8e8e8; overflow:hidden;">
    <table id="companyTable">
      <thead>
        <tr>
          <th>#</th>
          <th>Company</th>
          <th>Appearances</th>
          <th>Share</th>
        </tr>
      </thead>
      <tbody id="companyTbody">
        <tr><td colspan="4" class="no-data">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<div class="section">
  <div class="section-title" id="titleBreakdownTable">Weekly Breakdown Table</div>
  <div style="background:white; border-radius:10px; border:1px solid #e8e8e8; overflow:hidden; overflow-x:auto;">
    <table id="weekTable">
      <thead>
        <tr>
          <th class="sortable" data-col="0" data-dir="desc" onclick="sortWeekTable(this)" id="thPeriodCol">Week</th>
          <th class="sortable" data-col="1" data-dir="desc" onclick="sortWeekTable(this)">Matched</th>
          <th class="sortable" data-col="2" data-dir="desc" onclick="sortWeekTable(this)">Rejected</th>
          <th class="sortable" data-col="3" data-dir="desc" onclick="sortWeekTable(this)">Match Rate</th>
          <th class="sortable" data-col="4" data-dir="desc" onclick="sortWeekTable(this)">Avg Fit</th>
          <th class="sortable" data-col="5" data-dir="desc" onclick="sortWeekTable(this)">Target Co.</th>
          <th>Trend</th>
        </tr>
      </thead>
      <tbody id="weekTbody">
        <tr><td colspan="7" class="no-data">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<footer>
  Auto-generated by job_searcher.py &nbsp;&middot;&nbsp;
  Charts: Chart.js (CDN) &nbsp;&middot;&nbsp;
  Data: accumulated SQLite runs
</footer>

<script>
// ── Embedded analytics payload ────────────────────────────────────
// DATA shape: {{ week_labels, day_labels, all_weekly, all_daily, all, desired_score }}
const DATA = {data_json};
const CAT_COLORS = {cat_colors_json};
const SRC_COLORS = {src_colors_json};
const DESIRED_SCORE = (DATA && DATA.desired_score) ? DATA.desired_score : 0;

// ── Granularity state: "week" or "day" ────────────────────────────
let GRAN = 'week';   // active granularity

// Return the active label array for the current granularity.
function activeLabels() {{
  return GRAN === 'day' ? (DATA.day_labels || []) : (DATA.week_labels || []);
}}

// Return the base (pre-filter) aggregation for the current granularity.
function baseAgg() {{
  return GRAN === 'day' ? DATA.all_daily : DATA.all_weekly;
}}

// ── View state ────────────────────────────────────────────────────
// VIEW = active aggregation (possibly score-filtered)
// RANGE = [fromIdx, toIdx] inclusive indices into the active label array
let VIEW  = (DATA && DATA.all_weekly) ? DATA.all_weekly : ((DATA && DATA.all) ? DATA.all : null);
let RANGE = [0, 0];  // will be set during boot

// ── Guard: no data yet ────────────────────────────────────────────
const hasData = !!(DATA && DATA.week_labels && DATA.week_labels.length > 0 && VIEW);

// ── Chart instance registry ───────────────────────────────────────
const CHARTS = {{}};

function destroyAllCharts() {{
  for (const key in CHARTS) {{
    if (CHARTS[key]) {{ CHARTS[key].destroy(); CHARTS[key] = null; }}
  }}
}}

// ── Slice helper: extract [from..to] inclusive from an array ──────
function sl(arr) {{
  if (!arr) return [];
  return arr.slice(RANGE[0], RANGE[1] + 1);
}}

// ── Recompute rolling avg for the sliced window ───────────────────
function slRolling(jobsArr) {{
  const raw = sl(jobsArr);
  return raw.map((_, i) => {{
    const win = raw.slice(Math.max(0, i - 3), i + 1);
    return Math.round(win.reduce((a, b) => a + b, 0) / win.length * 100) / 100;
  }});
}}

// ── Granularity toggle ────────────────────────────────────────────
function setGranularity(gran, btn) {{
  if (!hasData) return;
  GRAN = gran;

  // Update button highlight
  document.querySelectorAll('.gran-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');

  // Update UI labels for range strip and breakdown table
  const isDay = (gran === 'day');
  document.getElementById('fromPeriodLabel').textContent = isDay ? 'From day' : 'From week';
  document.getElementById('toPeriodLabel').textContent   = isDay ? 'To day'   : 'To week';
  const thPeriod = document.getElementById('thPeriodCol');
  if (thPeriod) thPeriod.textContent = isDay ? 'Day' : 'Week';
  const titleBreakdown = document.getElementById('titleBreakdownTable');
  if (titleBreakdown) titleBreakdown.textContent = isDay ? 'Daily Breakdown Table' : 'Weekly Breakdown Table';

  // Update chart heading suffixes
  const suffix = isDay ? 'per day' : 'per week';
  [
    ['titleChartWeekly',  `Jobs found ${{suffix}} (matched vs rejected)`],
    ['titleChartNewJobs', `New jobs discovered ${{suffix}} (first appearance)`],
    ['titleChartSource',  `Source: LinkedIn vs Indeed ${{suffix}}`],
    ['titleChartTrend',   `Rolling 4-period average (matched jobs) — is the market improving?`],
    ['titleChartFit',     `Average fit score ${{suffix}}`],
    ['titleChartTarget',  `Target company appearances ${{suffix}}`],
    ['titleChartDesired', `Desired matches ${{suffix}} (score ≥ ${{DESIRED_SCORE}})`],
    ['titleChartRatio',   `Match rate: accepted / (accepted + rejected) ${{suffix}}`],
  ].forEach(([id, text]) => {{
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }});

  // Reset VIEW to the base aggregation for the new granularity (respecting any
  // active score filter).  refreshView() rebuilds VIEW from job_records when a
  // filter is set, otherwise it falls back to baseAgg() which now returns the
  // correct granularity payload.
  const minScore    = _getMinScore();
  const newJobScore = _getNewJobMinScore();
  if (minScore > 0 || newJobScore > 0) {{
    let recs = DATA.job_records || [];
    if (minScore > 0) recs = recs.filter(r => r.sc >= minScore);
    VIEW = _rebuildViewFromRecords(recs);
  }} else {{
    VIEW = baseAgg();
  }}

  // Re-initialise range UI for the new label set, then re-render.
  initRangeUI();
}}

// ── Range selector helpers ────────────────────────────────────────
function initRangeUI() {{
  if (!hasData) return;
  const labels = activeLabels();
  const from = document.getElementById('fromPeriod');
  const to   = document.getElementById('toPeriod');
  from.innerHTML = labels.map((l, i) => `<option value="${{i}}">${{l}}</option>`).join('');
  to.innerHTML   = labels.map((l, i) => `<option value="${{i}}">${{l}}</option>`).join('');
  document.getElementById('rangeStrip').style.display  = '';
  document.getElementById('granToggle').style.display  = '';

  // Default: weekly → last 8, daily → last 30
  const defaultN   = GRAN === 'day' ? 30 : 8;
  // Find the quick-range button that matches the default N.
  // Buttons are: Last 4, Last 8, Last 12, Last 30, All time
  const quickBtns = document.querySelectorAll('.range-btn');
  let defaultBtn = null;
  quickBtns.forEach(b => {{
    const match = b.textContent.match(/\\d+/);
    if (match && parseInt(match[0], 10) === defaultN) defaultBtn = b;
  }});
  setQuickRange(defaultN, defaultBtn);
}}

function applyRange(fromIdx, toIdx) {{
  const labels = activeLabels();
  RANGE[0] = Math.max(0, fromIdx);
  RANGE[1] = Math.min(labels.length - 1, toIdx);
  document.getElementById('fromPeriod').value = RANGE[0];
  document.getElementById('toPeriod').value   = RANGE[1];
}}

function setQuickRange(n, btn) {{
  // Highlight active button
  document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');

  const total = activeLabels().length;
  if (n === 0 || n >= total) {{
    applyRange(0, total - 1);
  }} else {{
    applyRange(total - n, total - 1);
  }}
  destroyAllCharts();
  renderAll();
}}

function onRangeDropdown() {{
  // Clear quick-range highlight when user manually picks dropdowns
  document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
  const from = parseInt(document.getElementById('fromPeriod').value, 10);
  const to   = parseInt(document.getElementById('toPeriod').value,   10);
  if (from <= to) {{
    RANGE[0] = from;
    RANGE[1] = to;
    destroyAllCharts();
    renderAll();
  }}
}}

// ── Stat cards ────────────────────────────────────────────────────
function renderStatCards() {{
  if (!hasData) return;
  const container = document.getElementById('statCards');

  // Compute stats for selected range
  const slMatched  = sl(VIEW.jobs_per_week);
  const slRejected = sl(VIEW.rejected_per_week);
  const slFit      = sl(VIEW.avg_fit_per_week);
  const slTarget   = sl(VIEW.target_per_week);

  const totalMatched  = slMatched.reduce((a, b) => a + b, 0);
  const totalRejected = slRejected.reduce((a, b) => a + b, 0);
  const totalTarget   = slTarget.reduce((a, b) => a + b, 0);
  const matchRate     = (totalMatched + totalRejected) > 0
    ? ((totalMatched / (totalMatched + totalRejected)) * 100).toFixed(1)
    : '—';
  const fitVals = slFit.filter(v => v > 0);
  const avgFit  = fitVals.length
    ? (fitVals.reduce((a, b) => a + b, 0) / fitVals.length).toFixed(2)
    : '—';

  // Trend: compare current range to equal-length prior period
  const rangeLen = RANGE[1] - RANGE[0] + 1;
  const prevStart = RANGE[0] - rangeLen;
  let trendHtml = '';
  if (prevStart >= 0) {{
    const prevMatched = VIEW.jobs_per_week.slice(prevStart, RANGE[0]).reduce((a, b) => a + b, 0);
    const delta = totalMatched - prevMatched;
    if (delta > 0)      trendHtml = `<span class="stat-trend up">&#8593; +${{delta}} vs prev period</span>`;
    else if (delta < 0) trendHtml = `<span class="stat-trend down">&#8595; ${{delta}} vs prev period</span>`;
    else                trendHtml = `<span class="stat-trend flat">&#8212; no change</span>`;
  }}

  const slDesired = DESIRED_SCORE > 0 ? sl(VIEW.desired_per_week || []) : [];
  const totalDesired = slDesired.reduce((a, b) => a + b, 0);

  const s = VIEW.summary;
  const _al = activeLabels();
  const rangeLabel = `${{_al[RANGE[0]]}} – ${{_al[RANGE[1]]}}`;

  container.innerHTML = `
    <div class="stat-card">
      <div class="stat-value">${{s.total_runs}}</div>
      <div class="stat-label">Total scrape runs</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${{totalMatched}}</div>
      <div class="stat-label">Jobs matched</div>
      ${{trendHtml}}
    </div>
    <div class="stat-card">
      <div class="stat-value">${{totalRejected}}</div>
      <div class="stat-label">Jobs rejected</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${{matchRate}}%</div>
      <div class="stat-label">Match rate</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${{avgFit}}</div>
      <div class="stat-label">Avg fit score</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${{totalTarget}}</div>
      <div class="stat-label">Target company hits</div>
    </div>
    ${{DESIRED_SCORE > 0 ? `
    <div class="stat-card">
      <div class="stat-value" style="color:#c0392b;">${{totalDesired}}</div>
      <div class="stat-label">Desired matches (score &ge; ${{DESIRED_SCORE}})</div>
    </div>` : ''}}
  `;
}}

// ── Chart defaults ────────────────────────────────────────────────
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.font.size   = 11;
Chart.defaults.color       = '#666';
const GRID_COLOR = 'rgba(0,0,0,0.06)';

function baseScales(stacked) {{
  return {{
    x: {{
      stacked: stacked || false,
      grid: {{ color: GRID_COLOR }},
      ticks: {{ maxRotation: 45 }},
    }},
    y: {{
      stacked: stacked || false,
      grid: {{ color: GRID_COLOR }},
      beginAtZero: true,
    }},
  }};
}}

function hexToRgba(hex, alpha) {{
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${{r}},${{g}},${{b}},${{alpha}})`;
}}

// ── Chart 1: Weekly matched + rejected bar ───────────────────────
function buildChartWeekly() {{
  if (!hasData) return;
  CHARTS.weekly = new Chart(document.getElementById('chartWeekly'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [
        {{
          label: 'Matched',
          data: sl(VIEW.jobs_per_week),
          borderColor: '#2e86ab',
          backgroundColor: '#2e86ab22',
          borderWidth: 2, pointRadius: 3, fill: true, tension: 0.3,
        }},
        {{
          label: 'Rejected / Blacklisted',
          data: sl(VIEW.rejected_per_week),
          borderColor: '#dc3545',
          backgroundColor: '#dc354522',
          borderWidth: 2, pointRadius: 3, fill: true, tension: 0.3,
        }},
      ],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart 2: Category stacked area line ──────────────────────────
function buildChartCategory() {{
  if (!hasData) return;
  const slLabels = sl(activeLabels());
  const datasets = Object.entries(VIEW.cat_per_week).map(([cat, vals]) => {{
    const color = CAT_COLORS[cat] || '#999999';
    return {{
      label: cat,
      data: sl(vals),
      borderColor: color,
      backgroundColor: hexToRgba(color.length === 7 ? color : '#999999', 0.18),
      borderWidth: 2,
      pointRadius: 2,
      fill: true,
      tension: 0.3,
    }};
  }});
  CHARTS.category = new Chart(document.getElementById('chartCategory'), {{
    type: 'line',
    data: {{ labels: slLabels, datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ boxWidth: 10, font: {{ size: 10 }} }} }} }},
      scales: baseScales(true),
    }},
  }});
}}

// ── Chart 3: Source line chart ────────────────────────────────────
function buildChartSource() {{
  if (!hasData) return;
  const slLabels = sl(activeLabels());
  const datasets = Object.entries(VIEW.src_per_week).map(([src, vals]) => {{
    const color = SRC_COLORS[src] || '#888888';
    return {{
      label: src.charAt(0).toUpperCase() + src.slice(1),
      data: sl(vals),
      borderColor: color,
      backgroundColor: hexToRgba(color, 0.08),
      borderWidth: 2,
      pointRadius: 3,
      fill: false,
      tension: 0.3,
    }};
  }});
  CHARTS.source = new Chart(document.getElementById('chartSource'), {{
    type: 'line',
    data: {{ labels: slLabels, datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart: New jobs discovered per period ────────────────────────
function buildChartNewJobs() {{
  if (!hasData) return;
  const newArr       = sl(VIEW.new_jobs_per_week || []);
  const totalArr     = sl(VIEW.jobs_per_week);
  const returningArr = totalArr.map((t, i) => Math.max(0, t - (newArr[i] || 0)));
  CHARTS.newJobs = new Chart(document.getElementById('chartNewJobs'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [
        {{
          label: 'New (first appearance)',
          data: newArr,
          borderColor: '#1a3a5c',
          backgroundColor: '#1a3a5c22',
          borderWidth: 2, pointRadius: 3, fill: true, tension: 0.3,
        }},
        {{
          label: 'Returning (seen before)',
          data: returningArr,
          borderColor: '#28a745',
          backgroundColor: '#28a74522',
          borderWidth: 2, pointRadius: 3, fill: true, tension: 0.3,
        }},
      ],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(true),
    }},
  }});
}}

// ── Chart 4: Rolling trend line ───────────────────────────────────
function buildChartTrend() {{
  if (!hasData) return;
  CHARTS.trend = new Chart(document.getElementById('chartTrend'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [
        {{
          label: 'Weekly matched',
          data: sl(VIEW.jobs_per_week),
          borderColor: '#2e86ab44',
          backgroundColor: '#2e86ab11',
          borderWidth: 1,
          pointRadius: 2,
          fill: true,
          tension: 0.3,
        }},
        {{
          label: '4-week rolling avg',
          data: slRolling(VIEW.jobs_per_week),
          borderColor: '#1a3a5c',
          backgroundColor: 'transparent',
          borderWidth: 2.5,
          pointRadius: 3,
          fill: false,
          tension: 0.3,
        }},
      ],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart 5: Avg fit score line ───────────────────────────────────
function buildChartFit() {{
  if (!hasData) return;
  CHARTS.fit = new Chart(document.getElementById('chartFit'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [{{
        label: 'Avg fit score (matched)',
        data: sl(VIEW.avg_fit_per_week),
        borderColor: '#28a745',
        backgroundColor: '#28a74511',
        borderWidth: 2,
        pointRadius: 3,
        fill: true,
        tension: 0.3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: {{
        ...baseScales(false),
        y: {{ ...baseScales(false).y, min: 0 }},
      }},
    }},
  }});
}}

// ── Chart 6: Target company count per period ─────────────────────
function buildChartTarget() {{
  if (!hasData) return;
  CHARTS.target = new Chart(document.getElementById('chartTarget'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [{{
        label: 'Target company matches',
        data: sl(VIEW.target_per_week),
        borderColor: '#f39c12',
        backgroundColor: '#f39c1222',
        borderWidth: 2, pointRadius: 3, fill: true, tension: 0.3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart: Desired matches per period ────────────────────────────
function buildChartDesired() {{
  const card = document.getElementById('chartDesiredCard');
  if (!hasData || DESIRED_SCORE <= 0) {{
    if (card) card.style.display = 'none';
    return;
  }}
  if (card) card.style.display = '';
  CHARTS.desired = new Chart(document.getElementById('chartDesired'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [{{
        label: `Desired matches (score ≥ ${{DESIRED_SCORE}})`,
        data: sl(VIEW.desired_per_week || []),
        borderColor: '#c0392b',
        backgroundColor: '#c0392b22',
        borderWidth: 2, pointRadius: 3, fill: true, tension: 0.3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart 7: Match rate line ──────────────────────────────────────
function buildChartRatio() {{
  if (!hasData) return;
  // Recompute ratio from sliced matched/rejected so the % is correct for the window
  const slMatched  = sl(VIEW.jobs_per_week);
  const slRejected = sl(VIEW.rejected_per_week);
  const pcts = slMatched.map((m, i) => {{
    const total = m + slRejected[i];
    return total ? Math.round(m / total * 1000) / 10 : 0;
  }});
  CHARTS.ratio = new Chart(document.getElementById('chartRatio'), {{
    type: 'line',
    data: {{
      labels: sl(activeLabels()),
      datasets: [{{
        label: 'Match rate (%)',
        data: pcts,
        borderColor: '#856404',
        backgroundColor: '#ffc10711',
        borderWidth: 2,
        pointRadius: 3,
        fill: true,
        tension: 0.3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: {{
        ...baseScales(false),
        y: {{ ...baseScales(false).y, min: 0, max: 100,
               ticks: {{ callback: v => v + '%' }} }},
      }},
    }},
  }});
}}

// ── Chart 8: Top companies horizontal bar ────────────────────────
function buildChartCompanies() {{
  if (!hasData) return;
  // Use full (non-sliced) top_companies from VIEW — the top-companies list
  // is pre-aggregated all-time but we display top 10 here
  const top10 = VIEW.top_companies.slice(0, 10);
  if (!top10.length) return;
  CHARTS.companies = new Chart(document.getElementById('chartCompanies'), {{
    type: 'bar',
    data: {{
      labels: top10.map(r => r.company),
      datasets: [{{
        label: 'Appearances',
        data: top10.map(r => r.count),
        backgroundColor: '#2e86ab',
        borderRadius: 4,
      }}],
    }},
    options: {{
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: GRID_COLOR }}, beginAtZero: true }},
        y: {{ grid: {{ color: 'transparent' }}, ticks: {{ font: {{ size: 11 }} }} }},
      }},
    }},
  }});
}}

// ── Top companies table ───────────────────────────────────────────
function buildCompanyTable() {{
  if (!hasData) return;
  const tbody = document.getElementById('companyTbody');
  if (!VIEW.top_companies.length) {{
    tbody.innerHTML = '<tr><td colspan="4" class="no-data">No companies in this view.</td></tr>';
    return;
  }}
  const maxCount  = VIEW.top_companies[0].count;
  const totalAcc  = VIEW.summary.total_accepted;
  tbody.innerHTML = VIEW.top_companies.map((row, i) => {{
    const pct   = maxCount ? Math.round(row.count / maxCount * 100) : 0;
    const share = totalAcc ? ((row.count / totalAcc) * 100).toFixed(1) + '%' : '—';
    return `<tr>
      <td>${{i + 1}}</td>
      <td>${{row.company}}</td>
      <td>
        <div class="bar-cell">
          <span>${{row.count}}</span>
          <div class="mini-bar"><div class="mini-bar-fill" style="width:${{pct}}%"></div></div>
        </div>
      </td>
      <td>${{share}}</td>
    </tr>`;
  }}).join('');
}}

// ── Period breakdown table (with sort) ───────────────────────────
let _weekSortCol = 0;   // default sort: Period column
let _weekSortDir = -1;  // -1 = desc (newest first)

function buildWeekTable() {{
  if (!hasData) return;
  const tbody  = document.getElementById('weekTbody');
  const labels = activeLabels();

  // Build row data for the selected range (newest first default)
  const rows = [];
  for (let i = RANGE[0]; i <= RANGE[1]; i++) {{
    const matched  = VIEW.jobs_per_week[i];
    const rejected = VIEW.rejected_per_week[i];
    const total    = matched + rejected;
    const matchPct = total ? ((matched / total) * 100) : null;
    const avgFit   = VIEW.avg_fit_per_week[i];
    const target   = VIEW.target_per_week[i];
    const rolling  = VIEW.rolling_avg[i];

    let trendBadge = '';
    if (i > 0) {{
      const prev  = VIEW.rolling_avg[i - 1];
      const delta = rolling - prev;
      if (delta > 0.3)       trendBadge = '<span class="trend-badge trend-up">improving</span>';
      else if (delta < -0.3) trendBadge = '<span class="trend-badge trend-down">declining</span>';
      else                   trendBadge = '<span class="trend-badge trend-flat">stable</span>';
    }}

    rows.push({{
      week:      labels[i],
      matched,
      rejected,
      matchPct:  matchPct !== null ? matchPct : -1,
      avgFit:    avgFit || 0,
      target,
      trendBadge,
      // sortable numeric values for each column (col index matches th data-col)
      sortVals: [labels[i], matched, rejected, matchPct !== null ? matchPct : -1, avgFit || 0, target],
    }});
  }}

  // Sort
  rows.sort((a, b) => {{
    const av = a.sortVals[_weekSortCol];
    const bv = b.sortVals[_weekSortCol];
    if (av < bv) return _weekSortDir;
    if (av > bv) return -_weekSortDir;
    return 0;
  }});

  tbody.innerHTML = rows.map(r => `<tr>
    <td>${{r.week}}</td>
    <td>${{r.matched}}</td>
    <td>${{r.rejected}}</td>
    <td>${{r.matchPct >= 0 ? r.matchPct.toFixed(0) + '%' : '—'}}</td>
    <td>${{r.avgFit || '—'}}</td>
    <td>${{r.target}}</td>
    <td>${{r.trendBadge}}</td>
  </tr>`).join('');
}}

function sortWeekTable(th) {{
  const col = parseInt(th.dataset.col, 10);
  // Toggle direction if same column, else reset to desc
  if (_weekSortCol === col) {{
    _weekSortDir = -_weekSortDir;
  }} else {{
    _weekSortCol = col;
    _weekSortDir = -1;
  }}
  // Update header classes
  document.querySelectorAll('#weekTable th.sortable').forEach(h => {{
    h.classList.remove('sort-asc', 'sort-desc');
  }});
  th.classList.add(_weekSortDir === -1 ? 'sort-desc' : 'sort-asc');
  buildWeekTable();
}}

// ── Render everything ─────────────────────────────────────────────
function renderAll() {{
  renderStatCards();
  buildChartWeekly();
  buildChartNewJobs();
  buildChartSource();
  buildChartTrend();
  buildChartFit();
  buildChartTarget();
  buildChartDesired();
  buildChartRatio();
  buildChartCompanies();
  buildCompanyTable();
  buildWeekTable();
}}

// ── Score-filter re-aggregation ───────────────────────────────────
// When a min-score filter is active we rebuild VIEW from DATA.job_records
// (individual accepted jobs) so the charts reflect exactly the filtered
// subset.  Rejected counts become 0 in this mode (rejected jobs have no
// score and are not in job_records).

const _ALL_CATS = [
  "GNC/Aerospace","High-Tech/Precision","Robotics","General Control",
  "Blacklisted-PLC","Blacklisted-Other","Rejected-Senior","Rejected-No-Gate"
];
const _SRCS = ["linkedin","indeed","other"];

function _rebuildViewFromRecords(recs) {{
  // recs = already-filtered subset of DATA.job_records
  // Uses GRAN to decide whether to bucket by ISO week (r.wk / r.fsw) or
  // calendar day (r.fsd — first-seen YYYY-MM-DD).
  const labels = activeLabels();
  const pkIdx  = {{}};
  labels.forEach((lbl, i) => pkIdx[lbl] = i);
  const n = labels.length;

  const isDay = (GRAN === 'day');

  const jobsArr     = new Array(n).fill(0);
  const fitSum      = new Array(n).fill(0);
  const fitCnt      = new Array(n).fill(0);
  const tgtArr      = new Array(n).fill(0);
  const desiredArr  = new Array(n).fill(0);
  const catArr      = Object.fromEntries(_ALL_CATS.map(c => [c, new Array(n).fill(0)]));
  const srcArr      = Object.fromEntries(_SRCS.map(s => [s, new Array(n).fill(0)]));
  const coCounts    = {{}};

  for (const r of recs) {{
    // Pick the period key depending on granularity.
    // For daily mode, r.fsd (first-seen date) is the bucket key.
    // For weekly mode, r.wk (run-week) is the bucket key.
    const pk = isDay ? r.fsd : r.wk;
    const i  = pkIdx[pk];
    if (i === undefined) continue;
    jobsArr[i]++;
    fitSum[i]  += r.sc;
    fitCnt[i]++;
    if (r.tgt) tgtArr[i]++;
    if (DESIRED_SCORE > 0 && r.sc >= DESIRED_SCORE) desiredArr[i]++;
    const cat = _ALL_CATS.includes(r.cat) ? r.cat : "General Control";
    catArr[cat][i]++;
    const src = _SRCS.includes(r.src) ? r.src : "other";
    srcArr[src][i]++;
    coCounts[r.co] = (coCounts[r.co] || 0) + 1;
  }}

  const rolling = jobsArr.map((_, i) => {{
    const win = jobsArr.slice(Math.max(0, i - 3), i + 1);
    return Math.round(win.reduce((a, b) => a + b, 0) / win.length * 100) / 100;
  }});
  const avgFitArr = jobsArr.map((_, i) =>
    fitCnt[i] ? Math.round(fitSum[i] / fitCnt[i] * 100) / 100 : 0
  );
  const topCo = Object.entries(coCounts)
    .sort((a, b) => b[1] - a[1]).slice(0, 20)
    .map(([company, count]) => ({{ company, count }}));
  const totalAcc  = recs.length;
  const avgFitAll = totalAcc
    ? Math.round(recs.reduce((s, r) => s + r.sc, 0) / totalAcc * 100) / 100
    : 0;

  // new_jobs: count per-period using the first-seen key (fsw for weekly, fsd for daily).
  // Apply the new-job score filter (separate dimension) from #newJobScoreFilter.
  const newJobMinScore = parseFloat(document.getElementById('newJobScoreFilter')?.value) || 0;
  const newJobsArr = labels.map(lbl =>
    (DATA.job_records || []).filter(r => {{
      const fsKey = isDay ? r.fsd : r.fsw;
      return fsKey === lbl && r.sc >= newJobMinScore;
    }}).length
  );

  const baseSum = isDay ? DATA.all_daily?.summary : DATA.all_weekly?.summary;
  return {{
    jobs_per_week:     jobsArr,
    rejected_per_week: new Array(n).fill(0),
    rolling_avg:       rolling,
    avg_fit_per_week:  avgFitArr,
    target_per_week:   tgtArr,
    desired_per_week:  desiredArr,
    cat_per_week:      catArr,
    src_per_week:      srcArr,
    top_companies:     topCo,
    ratio_per_week:    jobsArr.map(m => ({{ accepted: m, rejected: 0, total: m, accept_pct: 100 }})),
    new_jobs_per_week: newJobsArr,
    summary: {{
      total_seen:     totalAcc,
      total_accepted: totalAcc,
      total_rejected: 0,
      avg_fit:        avgFitAll,
      total_desired:  desiredArr.reduce((a, b) => a + b, 0),
      total_runs:     baseSum?.total_runs || 0,
      last_run:       baseSum?.last_run   || '',
    }},
  }};
}}

function _getMinScore() {{
  const v = parseFloat(document.getElementById('minScoreFilter').value);
  return isNaN(v) ? 0 : v;
}}

function _getNewJobMinScore() {{
  const v = parseFloat(document.getElementById('newJobScoreFilter').value);
  return isNaN(v) ? 0 : v;
}}

// ── Unified view refresh (score filter) ───────────────────────────
function refreshView() {{
  if (!hasData) return;
  const minScore    = _getMinScore();
  const newJobScore = _getNewJobMinScore();

  if (minScore > 0 || newJobScore > 0) {{
    // Filter individual records and re-aggregate
    let recs = DATA.job_records || [];
    if (minScore > 0) {{
      recs = recs.filter(r => r.sc >= minScore);
    }}
    VIEW = _rebuildViewFromRecords(recs);
  }} else {{
    // No filters active — use the fast pre-aggregated payload for current granularity.
    VIEW = baseAgg();
  }}

  destroyAllCharts();
  renderAll();
}}

function onScoreFilter()       {{ if (hasData) refreshView(); }}
function onNewJobScoreFilter() {{ if (hasData) refreshView(); }}

// ── Boot ──────────────────────────────────────────────────────────
if (hasData) {{
  initRangeUI();  // sets RANGE and calls renderAll() via setQuickRange
}} else {{
  renderAll();    // renders "no data" state
}}
</script>
</body>
</html>"""

    out_path = os.path.join(OUTPUT_DIR, "analytics.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Analytics page generated: {out_path}")


# ─────────────────────────────────────────────────────────────────
# OUTPUT HELPERS
# ─────────────────────────────────────────────────────────────────

def _format_date(row) -> str:
    """Format date_posted safely, falling back to first_seen scrape date."""
    def _parse(col) -> str | None:
        try:
            if col is None or col == "" or (isinstance(col, float) and pd.isna(col)):
                return None
            if hasattr(col, "strftime"):
                return col.strftime("%d %b %Y")
            s = str(col).strip()[:10]
            return s if s else None
        except Exception:
            return None

    # 1. Try date_posted (when the company posted the job)
    result = _parse(row.get("date_posted") or row.get("date"))
    if result:
        return result

    # 2. Fall back to first_seen (when we first scraped it) — mark as approximate
    fallback = _parse(row.get("first_seen") or row.get("run_date"))
    if fallback:
        return f"~{fallback}"

    return "Date unknown"


def _score_bar(score: float, width: int = 10) -> str:
    """Return a simple ASCII progress bar for fit score."""
    filled = round(score / _MAX_POSSIBLE_SCORE * width)
    filled = max(0, min(filled, width))
    return "[" + "#" * filled + "-" * (width - filled) + f"] {score:.1f}"


# ─────────────────────────────────────────────────────────────────
# TERMINAL OUTPUT
# ─────────────────────────────────────────────────────────────────

def print_results(df: pd.DataFrame):
    """Print ranked numbered list to terminal."""
    print(f"\n{'='*68}")
    print(f"  CONTROL SYSTEMS JOB RESULTS — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Indeed NL + LinkedIn | Fresh grad filter ON | Blacklist: PLC, SCADA, HMI...")
    print(f"{'='*68}\n")

    if df.empty:
        print("  No relevant jobs found. Try again tomorrow.")
        return

    for i, (_, row) in enumerate(df.iterrows(), 1):
        star     = "* " if row.get("is_target") else "  "
        title    = str(row.get("title",    "Unknown title"))
        company  = str(row.get("company",  "Unknown company"))
        location = str(row.get("location", "Unknown location"))
        site     = str(row.get("site",     "")).upper()
        link     = str(row.get("job_url",  "No link"))
        date     = _format_date(row)
        score    = float(row.get("fit_score", 0))
        keywords = ", ".join(row.get("matched_keywords", [])[:6])
        bar      = _score_bar(score)

        print(f"{i:>3}. {star}{title}")
        print(f"       {company} | {location} | {site}")
        print(f"       Fit: {bar}")
        print(f"       Date: {date}")
        print(f"       {link}")
        print(f"       Keywords: {keywords}")
        print()




# ─────────────────────────────────────────────────────────────────
# CSV SAVE
# ─────────────────────────────────────────────────────────────────

def save_results(df: pd.DataFrame):
    if df.empty:
        return
    cols_wanted = [
        "title", "company", "location", "site",
        "fit_score", "is_target", "date_posted",
        "job_url", "matched_keywords",
    ]
    cols = [c for c in cols_wanted if c in df.columns]
    filename = os.path.join(OUTPUT_DIR, f"jobs_{datetime.now().strftime('%Y%m%d')}.csv")
    df[cols].to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Saved {len(df)} jobs to {filename}")


# ─────────────────────────────────────────────────────────────────
# GITHUB PAGES SITE
# ─────────────────────────────────────────────────────────────────

def generate_site(df: pd.DataFrame):
    """Generate a self-contained index.html for GitHub Pages.

    All accepted jobs in jobs.db are always shown on the dashboard.
    The user manages visibility via Discard and Applied buttons only.
    df (current run) is kept as a fallback when the DB is empty.
    """
    updated  = datetime.now().strftime("%d %B %Y at %H:%M UTC")
    today    = datetime.now().strftime("%Y-%m-%d")

    # ── DB queries (previously_seen, alltime_max, all accepted jobs) ──
    previously_seen: set[str] = set()   # job_ids first seen before today
    alltime_max: float = 0.0
    # first_seen_map: job_id -> first-seen date string
    first_seen_map: dict[str, str] = {}
    display_df: pd.DataFrame = pd.DataFrame()

    if os.path.exists(DB_PATH):
        try:
            _conn = sqlite3.connect(DB_PATH)

            # All-time best score benchmark
            _max_row = _conn.execute(
                "SELECT MAX(fit_score) FROM jobs WHERE fit_score IS NOT NULL"
            ).fetchone()
            if _max_row and _max_row[0] is not None:
                alltime_max = float(_max_row[0])

            # Query ALL accepted jobs — no date filter
            window_rows = _conn.execute(
                """
                SELECT job_id, title, company, location, job_url, site,
                       fit_score, matched_keywords, is_target,
                       MIN(run_date) AS first_seen,
                       MAX(date_posted) AS date_posted,
                       MAX(description) AS description
                FROM   jobs
                WHERE  (rejection_reason IS NULL OR rejection_reason = '')
                GROUP  BY job_id
                ORDER  BY is_target DESC, fit_score DESC
                """
            ).fetchall()
            _conn.close()

            if window_rows:
                cols = [
                    "job_id", "title", "company", "location",
                    "job_url", "site", "fit_score", "matched_keywords",
                    "is_target", "first_seen", "date_posted", "description",
                ]
                display_df = pd.DataFrame(window_rows, columns=cols)
                display_df["is_target"] = display_df["is_target"].astype(bool)
                # Build first_seen_map and previously_seen from the query data
                for _, r in display_df.iterrows():
                    first_seen_map[r["job_id"]] = r["first_seen"]
                    if r["first_seen"] < today:
                        previously_seen.add(r["job_id"])

        except Exception as exc:
            print(f"  [site] Could not query DB for dashboard: {exc}")
            # Fall back silently — display_df stays empty, previously_seen stays empty

    # Fall back to current-run df when DB query produced nothing
    if display_df.empty:
        display_df = df.copy() if not df.empty else pd.DataFrame()

    # If DB had no score data, fall back to current run's best, then theoretical max
    if alltime_max == 0.0 and not df.empty:
        alltime_max = float(df["fit_score"].max()) if "fit_score" in df.columns else 0.0
    if alltime_max == 0.0:
        alltime_max = _MAX_POSSIBLE_SCORE if _MAX_POSSIBLE_SCORE > 0 else 1.0
    SCORE_REF = alltime_max

    # ── Read applied.json ─────────────────────────────────────────────
    applied_ids: set[str] = set()
    applied_json_path = os.path.join(OUTPUT_DIR, "applied.json")
    if os.path.exists(applied_json_path):
        try:
            with open(applied_json_path, "r", encoding="utf-8") as _f:
                applied_ids = set(json.load(_f))
        except Exception:
            pass

    discarded_ids: set[str] = set()
    discarded_json_path = os.path.join(OUTPUT_DIR, "discarded.json")
    if os.path.exists(discarded_json_path):
        try:
            with open(discarded_json_path, "r", encoding="utf-8") as _f:
                discarded_ids = set(json.load(_f))
        except Exception:
            pass

    maybe_ids: set[str] = set()
    maybe_json_path = os.path.join(OUTPUT_DIR, "maybe.json")
    if os.path.exists(maybe_json_path):
        try:
            with open(maybe_json_path, "r", encoding="utf-8") as _f:
                maybe_ids = set(json.load(_f))
        except Exception:
            pass

    total    = len(display_df)
    target_n = int(display_df["is_target"].sum()) if not display_df.empty else 0
    high_fit = int((display_df["fit_score"] >= 0.4 * SCORE_REF).sum()) if not display_df.empty else 0

    # Count new jobs today that meet the desired score threshold (for banner).
    # "New" means first_seen == today (for DB window) or not in previously_seen (fallback).
    desired_new_count = 0
    if DESIRED_SCORE > 0 and not display_df.empty:
        for _, row in display_df.iterrows():
            title   = str(row.get("title",   "Unknown"))
            company = str(row.get("company", "Unknown"))
            link    = str(row.get("job_url", "#"))
            jid     = _job_id(title, company, link)
            # A job is "new" if its first_seen is today OR it was not seen before today
            if first_seen_map:
                is_new_job = first_seen_map.get(jid, today) == today
            else:
                is_new_job = jid not in previously_seen
            if is_new_job and float(row.get("fit_score", 0)) >= DESIRED_SCORE:
                desired_new_count += 1

    # Notification banner (only shown when desired_score is active and there are matches)
    banner_html = ""
    if DESIRED_SCORE > 0 and desired_new_count > 0:
        label = "top match" if desired_new_count == 1 else "top matches"
        banner_html = (
            f'<div class="top-match-banner">'
            f'&#128293; {desired_new_count} new {label} today '
            f'(score &ge; {DESIRED_SCORE})'
            f'</div>'
        )

    cards_html = ""
    if display_df.empty:
        cards_html = '<div class="empty"><p>No relevant jobs found today. Check back tomorrow.</p></div>'
    else:
        for _, row in display_df.iterrows():
            is_target  = bool(row.get("is_target", False))
            score      = float(row.get("fit_score", 0))
            title      = str(row.get("title",    "Unknown"))
            company    = str(row.get("company",  "Unknown"))
            location   = str(row.get("location", ""))
            site_name  = str(row.get("site",     "")).lower()
            link       = str(row.get("job_url",  "#"))
            date       = _format_date(row)
            raw_kws = row.get("matched_keywords", [])
            if isinstance(raw_kws, str):
                try:
                    raw_kws = json.loads(raw_kws)
                except Exception:
                    raw_kws = []
            kws = [str(k) for k in (raw_kws or []) if k]
            if not kws:
                # Fallback: recompute from title text
                _title_text = str(row.get("title", "")).lower()
                _, kws = _compute_fit_score(_title_text)
            keywords = ", ".join(kws[:10]) if kws else ""

            desc_text = str(row.get("description", "") or "").strip()
            desc_display = (desc_text[:200] + "...") if len(desc_text) > 200 else desc_text
            desc_html = f'<div class="job-desc">{desc_display}</div>' if desc_display else ""

            # New vs returning detection.
            # If first_seen_map is populated (DB window mode):
            #   new  = first_seen date is today
            #   seen = first_seen date is before today (job persisted from earlier run)
            # Fallback (current-run mode): new = jid not in previously_seen set.
            jid = _job_id(title, company, link)
            if first_seen_map:
                is_new = first_seen_map.get(jid, today) == today
            else:
                is_new = jid not in previously_seen

            # Salary — surfaced if jobspy returns it
            salary_str = ""
            if row.get("min_amount") and row.get("max_amount"):
                currency = str(row.get("currency", "EUR"))
                salary_str = f"{currency} {int(row['min_amount']):,} – {int(row['max_amount']):,}"
            elif row.get("min_amount"):
                currency = str(row.get("currency", "EUR"))
                salary_str = f"From {currency} {int(row['min_amount']):,}"

            # Score badge colour — thresholds relative to all-time best score
            if score >= 0.6 * SCORE_REF:
                score_cls = "score-high"
            elif score >= 0.3 * SCORE_REF:
                score_cls = "score-mid"
            else:
                score_cls = "score-low"

            # Score bar (HTML) — width as % of all-time best score
            bar_pct = min(score / SCORE_REF * 100, 100)
            bar_html = (
                f'<div class="score-bar">'
                f'<div class="bar-fill {score_cls}" style="width:{bar_pct:.1f}%"></div>'
                f'</div>'
            )

            is_desired = DESIRED_SCORE > 0 and score >= DESIRED_SCORE

            star_badge     = '<span class="badge badge-target">Target company</span>' if is_target else ""
            site_badge     = f'<span class="badge badge-{site_name}">{site_name.upper()}</span>'
            salary_badge   = f'<span class="badge badge-salary">{salary_str}</span>' if salary_str else ""
            newness_badge  = '<span class="badge badge-new">NEW</span>' if is_new else '<span class="badge badge-seen">Seen</span>'
            desired_badge  = f'<span class="badge badge-desired">&#9733; Match</span>' if is_desired else ""

            # Card classes and inline style
            is_applied_card   = jid in applied_ids
            is_discarded_card = jid in discarded_ids
            is_maybe_card     = jid in maybe_ids
            card_classes = "card"
            if is_target:
                card_classes += " target-card"
            if not is_new:
                card_classes += " returning-card"
            if is_applied_card:
                card_classes += " applied-card"
            if is_discarded_card:
                card_classes += " discarded-card"
            if is_maybe_card:
                card_classes += " maybe-card"
            if is_applied_card or is_discarded_card or is_maybe_card:
                card_style = ' style="display:none"'
            else:
                card_style = ""

            discard_btn_cls = 'discard-btn discarded' if is_discarded_card else 'discard-btn'
            maybe_btn_cls   = 'maybe-btn maybe' if is_maybe_card else 'maybe-btn'

            cards_html += f"""
<div class="{card_classes}"{card_style} data-score="{score}" data-site="{site_name}" data-target="{str(is_target).lower()}" data-new="{str(is_new).lower()}" data-desired="{str(is_desired).lower()}" data-jobid="{jid}">
  <div class="card-header">
    <h3><a href="{link}" target="_blank" rel="noopener">{title}</a></h3>
    <div class="badges">{desired_badge}{star_badge}{newness_badge}{site_badge}{salary_badge}</div>
  </div>
  <div class="card-meta">
    <span>{company}</span>
    <span>{location}</span>
    <span>{date}</span>
  </div>
  <div class="score-row">
    <span class="score-label">Fit:</span>
    <span class="score-num {score_cls}">{score}</span>
    {bar_html}
  </div>
  <div class="keywords">Keywords: {keywords}</div>
  {desc_html}
  <div style="display:flex; gap:8px; align-items:center; margin-top:4px;">
    <a class="apply-btn" href="{link}" target="_blank" rel="noopener">View posting</a>
    <button class="apply-toggle-btn" onclick="toggleAppliedCard(this)" data-jobid="{jid}">&#10003; Applied</button>
    <button class="{discard_btn_cls}" onclick="toggleDiscardCard(this)" data-jobid="{jid}">&#128465; Discard</button>
    <button class="{maybe_btn_cls}" onclick="toggleMaybeCard(this)" data-jobid="{jid}">? Maybe</button>
  </div>
</div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{SITE_TITLE}</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       background: #f0f2f5; color: #2c3e50; }}

header {{ background: #1a3a5c; color: white; padding: 22px 32px;
          display: flex; justify-content: space-between; align-items: center;
          flex-wrap: wrap; gap: 10px; }}
header h1 {{ font-size: 21px; font-weight: 600; }}
header p  {{ font-size: 12px; opacity: 0.65; margin-top: 5px; }}
.nav-link {{
  color: rgba(255,255,255,0.85); font-size: 13px; font-weight: 500;
  text-decoration: none; border: 1px solid rgba(255,255,255,0.4);
  padding: 6px 14px; border-radius: 6px; white-space: nowrap;
}}
.nav-link:hover {{ background: rgba(255,255,255,0.15); }}

.stats {{
  background: white; border-bottom: 1px solid #e0e0e0;
  padding: 12px 32px; font-size: 13px; color: #666;
  display: flex; gap: 28px; align-items: center; flex-wrap: wrap;
}}
.stats strong {{ color: #1a3a5c; font-size: 20px; }}

.controls {{
  padding: 14px 32px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center;
  background: white; border-bottom: 1px solid #eee;
}}
.controls input {{
  padding: 8px 14px; border: 1px solid #ccc; border-radius: 6px;
  font-size: 13px; width: 260px; outline: none;
}}
.controls input:focus {{ border-color: #1a3a5c; }}
.filter-btn {{
  padding: 7px 14px; border: 1px solid #ccc; border-radius: 6px;
  background: white; cursor: pointer; font-size: 12px; font-weight: 500;
}}
.filter-btn.active {{ background: #1a3a5c; color: white; border-color: #1a3a5c; }}
.sort-label {{ font-size: 12px; color: #888; margin-left: 8px; }}

.grid {{
  padding: 20px 32px 40px;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
  gap: 16px;
}}

.card {{
  background: white; border-radius: 10px; padding: 18px 20px;
  border: 1px solid #e8e8e8; transition: box-shadow 0.2s;
  display: flex; flex-direction: column; gap: 8px;
}}
.card:hover {{ box-shadow: 0 4px 18px rgba(0,0,0,0.09); }}
.target-card {{ border-left: 4px solid #f39c12; }}

.card-header {{
  display: flex; justify-content: space-between;
  align-items: flex-start; gap: 8px;
}}
.card-header h3 {{ font-size: 14px; font-weight: 600; line-height: 1.35; }}
.card-header h3 a {{ color: #1a3a5c; text-decoration: none; }}
.card-header h3 a:hover {{ text-decoration: underline; }}
.badges {{ display: flex; flex-direction: column; gap: 3px; flex-shrink: 0; align-items: flex-end; }}

.badge {{
  font-size: 10px; padding: 2px 7px; border-radius: 10px;
  white-space: nowrap; font-weight: 500;
}}
.badge-target  {{ background: #fef3cd; color: #856404; }}
.badge-linkedin {{ background: #e8f4fd; color: #0a66c2; }}
.badge-indeed   {{ background: #e8f8f0; color: #2e7d32; }}
.badge-salary   {{ background: #f3e8ff; color: #6b21a8; }}
.badge-new  {{ background: #dbeafe; color: #1d4ed8; }}
.badge-seen {{ background: #dcfce7; color: #166534; }}
.badge-desired {{ background: #fde8e8; color: #c0392b; border: 1px solid #e74c3c; font-weight: 700; }}
.returning-card {{ background: #f0fff4 !important; border-left: 4px solid #28a745; }}
.target-card.returning-card {{ border-left: 4px solid #f39c12; }}
.applied-card {{ opacity: 0.5; border-left: 4px solid #adb5bd !important; }}
.applied-card h3 a {{ text-decoration: line-through; color: #888 !important; }}
.apply-toggle-btn {{
  display: inline-block; margin-top: 4px; margin-left: 6px;
  padding: 5px 12px; background: #e2e8f0;
  color: #334155; border-radius: 6px; font-size: 11px;
  border: none; cursor: pointer; font-weight: 500;
}}
.apply-toggle-btn:hover {{ background: #cbd5e1; }}
.apply-toggle-btn.applied {{ background: #166534; color: white; }}
.discard-btn {{ padding:5px 10px; border-radius:5px; border:1px solid #e74c3c; background:white; color:#e74c3c; font-size:12px; font-weight:600; cursor:pointer; transition:background 0.15s, color 0.15s; }}
.discard-btn:hover {{ background:#e74c3c; color:white; }}
.discard-btn.discarded {{ background:#e74c3c; color:white; }}
.discarded-card {{ opacity:0.5; }}
.maybe-btn {{ padding:5px 10px; border-radius:5px; border:1px solid #f39c12; background:white; color:#f39c12; font-size:12px; font-weight:600; cursor:pointer; transition:background 0.15s,color 0.15s; }}
.maybe-btn:hover {{ background:#f39c12; color:white; }}
.maybe-btn.maybe {{ background:#f39c12; color:white; }}
.maybe-card {{ opacity:0.6; }}

.top-match-banner {{
  background: #fde8e8; border-bottom: 2px solid #e74c3c;
  padding: 10px 32px; font-size: 13px; font-weight: 600; color: #c0392b;
  display: flex; align-items: center; gap: 8px;
}}

.card-meta {{
  display: flex; flex-wrap: wrap; gap: 10px;
  font-size: 12px; color: #666;
}}

.score-row {{
  display: flex; align-items: center; gap: 8px; font-size: 12px;
}}
.score-label {{ color: #888; flex-shrink: 0; }}
.score-num   {{ font-weight: 700; flex-shrink: 0; min-width: 36px; }}
.score-high  {{ color: #1e7e34; }}
.score-mid   {{ color: #856404; }}
.score-low   {{ color: #721c24; }}

.score-bar {{
  flex: 1; height: 6px; background: #e8e8e8;
  border-radius: 4px; overflow: hidden;
}}
.bar-fill {{
  height: 100%; border-radius: 4px;
  transition: width 0.3s;
}}
.bar-fill.score-high {{ background: #28a745; }}
.bar-fill.score-mid  {{ background: #ffc107; }}
.bar-fill.score-low  {{ background: #dc3545; }}

.keywords {{ font-size: 11px; color: #999; }}
.job-desc {{ font-size: 12px; color: #666; margin-top: 6px; line-height: 1.5; }}

.apply-btn {{
  display: inline-block; margin-top: 4px;
  padding: 7px 16px; background: #1a3a5c;
  color: white; border-radius: 6px; font-size: 12px;
  text-decoration: none; font-weight: 500; align-self: flex-start;
}}
.apply-btn:hover {{ background: #2e6da4; }}

.empty {{ text-align: center; padding: 60px; color: #888; font-size: 15px; }}
.hidden {{ display: none !important; }}

footer {{ text-align: center; padding: 24px; font-size: 11px; color: #bbb; }}

@media (max-width: 600px) {{
  .grid {{ padding: 12px 16px; grid-template-columns: 1fr; }}
  header, .stats, .controls {{ padding-left: 16px; padding-right: 16px; }}
}}
</style>
</head>
<body>

<header>
  <div>
    <h1>{SITE_TITLE}</h1>
    <p>Updated: {updated}</p>
  </div>
  <div style="display:flex; gap:8px; flex-wrap:wrap;">
    <a class="nav-link" href="analytics.html">Analytics Dashboard</a>
    <a class="nav-link" href="archive.html">Archive</a>
    <a class="nav-link" href="settings.html">&#9881; Settings</a>
  </div>
</header>

{banner_html}

<div class="stats">
  <div><strong>{total}</strong> jobs found</div>
  <div><strong>{target_n}</strong> target companies</div>
  <div><strong>{high_fit}</strong> high fit (score &ge; {0.4 * SCORE_REF:.0f})</div>
  <div style="font-size:12px;">Sources: Indeed NL + LinkedIn</div>
  <div style="margin-left:auto;">
    <a id="appliedToggle" href="#" onclick="toggleApplied(event)"
       style="font-size:12px; color:#1a3a5c; font-weight:500; text-decoration:none;">
      Show applied (<span id="appliedCount">0</span>)
    </a>
  </div>
</div>

<div id="patNotice" style="font-size:12px;color:#aaa;padding:2px 32px 6px;display:none;">
  Applied jobs saved locally only — <a href="settings.html" style="color:#2e86ab;">connect GitHub in Settings</a> to sync to repo.
</div>

<div class="controls">
  <input type="text" id="searchBox" placeholder="Filter by title, company, keyword..."
         oninput="applyFilters()">
  <input type="number" id="minScoreBox" placeholder="Min score" min="0" step="1"
         title="Show only jobs with fit score >= this value"
         oninput="applyFilters()"
         style="width: 110px;">
  <button class="filter-btn active" onclick="filterSite('all', this)">All</button>
  <button class="filter-btn" onclick="filterSite('linkedin', this)">LinkedIn</button>
  <button class="filter-btn" onclick="filterSite('indeed', this)">Indeed</button>
  <button class="filter-btn" onclick="filterTarget(this)">Target only</button>
  <button class="filter-btn" onclick="filterNew(this)">New only</button>
  <span class="sort-label">Sort:</span>
  <button class="filter-btn active" onclick="sortCards('score', this)">Fit score</button>
  <button class="filter-btn" onclick="sortCards('date', this)">Newest</button>
</div>

<div class="grid" id="jobGrid">
{cards_html}
</div>

<footer>
  Auto-generated by job_searcher.py &nbsp;&middot;&nbsp;
  Runs daily at 09:00 NL time via GitHub Actions &nbsp;&middot;&nbsp;
  Fit score = raw weighted keyword depth (all-time best: {SCORE_REF:.0f})
</footer>

<script>
  const DESIRED_SCORE = {DESIRED_SCORE};
  const SERVER_APPLIED = new Set({json.dumps(list(applied_ids))});
  const SERVER_DISCARDED = new Set({json.dumps(list(discarded_ids))});
  const SERVER_MAYBE = new Set({json.dumps(list(maybe_ids))});
  let activeSite   = 'all';
  let targetOnly   = false;
  let newOnly      = false;
  let activeSort   = 'score';
  let showApplied  = false;

  // ── Applied jobs — persisted to applied.json in the repo ──────────
  const LS_KEY       = 'jss_applied';
  const LS_PAT       = 'jss_gh_pat';
  const LS_USER      = 'jss_gh_user';
  const LS_REPO      = 'jss_gh_repo';
  const APPLIED_FILE = 'applied.json';

  let appliedSet = new Set(SERVER_APPLIED);  // seed from server-rendered set

  async function loadApplied() {{
    // Fetch latest applied.json from GitHub Pages (cache-bust)
    try {{
      const res = await fetch(APPLIED_FILE + '?_=' + Date.now());
      if (res.ok) {{ const arr = await res.json(); arr.forEach(id => appliedSet.add(id)); }}
    }} catch(e) {{}}
    // Merge localStorage fallback
    try {{
      const local = JSON.parse(localStorage.getItem(LS_KEY) || '[]');
      local.forEach(id => appliedSet.add(id));
    }} catch(e) {{}}
    initAppliedUI();
  }}

  function initAppliedUI() {{
    document.querySelectorAll('[data-jobid]').forEach(card => {{
      if (appliedSet.has(card.dataset.jobid)) {{
        card.classList.add('applied-card');
        if (!window._showApplied) card.style.display = 'none';
        const btn = card.querySelector('.apply-toggle-btn');
        if (btn) {{ btn.textContent = '↩ Undo'; btn.classList.add('applied'); }}
      }}
    }});
    refreshAppliedCounter();
    checkPatNotice();
  }}

  async function saveApplied() {{
    const arr = Array.from(appliedSet);
    localStorage.setItem(LS_KEY, JSON.stringify(arr));   // always save locally too

    const pat  = localStorage.getItem(LS_PAT);
    const user = localStorage.getItem(LS_USER);
    const repo = localStorage.getItem(LS_REPO);
    if (!pat || !user || !repo) return;   // no PAT — localStorage only

    try {{
      const apiUrl = `https://api.github.com/repos/${{user}}/${{repo}}/contents/${{APPLIED_FILE}}`;
      let sha = null;
      try {{
        const r = await fetch(apiUrl, {{ headers: {{ Authorization: `Bearer ${{pat}}` }} }});
        if (r.ok) {{ const d = await r.json(); sha = d.sha; }}
      }} catch(e) {{}}
      const body = {{ message: 'Update applied.json', content: btoa(JSON.stringify(arr, null, 2)) }};
      if (sha) body.sha = sha;
      await fetch(apiUrl, {{
        method: 'PUT',
        headers: {{ Authorization: `Bearer ${{pat}}`, 'Content-Type': 'application/json' }},
        body: JSON.stringify(body)
      }});
    }} catch(e) {{ console.warn('Could not save applied.json:', e); }}
  }}

  function checkPatNotice() {{
    const el = document.getElementById('patNotice');
    if (el) el.style.display = localStorage.getItem(LS_PAT) ? 'none' : 'block';
  }}

  function refreshAppliedCounter() {{
    const count = document.querySelectorAll('[data-jobid].applied-card').length;
    const el = document.getElementById('appliedCount');
    if (el) el.textContent = count;
  }}

  function toggleAppliedCard(btn) {{
    const jid  = btn.dataset.jobid;
    const card = btn.closest('[data-jobid]');
    if (appliedSet.has(jid)) {{
      appliedSet.delete(jid);
      card.classList.remove('applied-card');
      card.style.display = '';
      btn.textContent = '✓ Applied';
      btn.classList.remove('applied');
    }} else {{
      appliedSet.add(jid);
      card.classList.add('applied-card');
      if (!window._showApplied) card.style.display = 'none';
      btn.textContent = '↩ Undo';
      btn.classList.add('applied');
    }}
    refreshAppliedCounter();
    saveApplied();
  }}

  function toggleApplied(e) {{
    e.preventDefault();
    window._showApplied = !window._showApplied;
    document.querySelectorAll('[data-jobid].applied-card').forEach(c => {{
      c.style.display = window._showApplied ? '' : 'none';
    }});
    const link = document.getElementById('appliedToggle');
    if (link) link.textContent = (window._showApplied ? 'Hide' : 'Show') + ' applied (' + document.getElementById('appliedCount').textContent + ')';
  }}

  // ── Discarded jobs ────────────────────────────────────────────────
  const DISCARD_FILE = 'discarded.json';
  let discardedSet = new Set(SERVER_DISCARDED);

  async function loadDiscarded() {{
    try {{
      const res = await fetch(DISCARD_FILE + '?_=' + Date.now());
      if (res.ok) {{ const arr = await res.json(); arr.forEach(id => discardedSet.add(id)); }}
    }} catch(e) {{}}
    try {{
      const local = JSON.parse(localStorage.getItem('jss_discarded') || '[]');
      local.forEach(id => discardedSet.add(id));
    }} catch(e) {{}}
    initDiscardedUI();
  }}

  function initDiscardedUI() {{
    document.querySelectorAll('[data-jobid]').forEach(card => {{
      if (discardedSet.has(card.dataset.jobid)) {{
        card.classList.add('discarded-card');
        card.style.display = 'none';
        const btn = card.querySelector('.discard-btn');
        if (btn) {{ btn.classList.add('discarded'); btn.title = 'Restore'; }}
      }}
    }});
  }}

  async function saveDiscarded() {{
    const arr = Array.from(discardedSet);
    localStorage.setItem('jss_discarded', JSON.stringify(arr));
    const pat  = localStorage.getItem(LS_PAT);
    const user = localStorage.getItem(LS_USER);
    const repo = localStorage.getItem(LS_REPO);
    if (!pat || !user || !repo) return;
    try {{
      const apiUrl = `https://api.github.com/repos/${{user}}/${{repo}}/contents/${{DISCARD_FILE}}`;
      let sha = null;
      try {{ const r = await fetch(apiUrl, {{headers:{{Authorization:`Bearer ${{pat}}`}}}}); if(r.ok){{const d=await r.json();sha=d.sha;}} }} catch(e) {{}}
      const body = {{message:'Update discarded.json', content:btoa(JSON.stringify(arr,null,2))}};
      if (sha) body.sha = sha;
      await fetch(apiUrl, {{method:'PUT', headers:{{Authorization:`Bearer ${{pat}}`,'Content-Type':'application/json'}}, body:JSON.stringify(body)}});
    }} catch(e) {{ console.warn('Could not save discarded.json:', e); }}
  }}

  function toggleDiscardCard(btn) {{
    const jid  = btn.dataset.jobid;
    const card = btn.closest('[data-jobid]');
    if (discardedSet.has(jid)) {{
      discardedSet.delete(jid);
      card.classList.remove('discarded-card');
      card.style.display = '';
      btn.classList.remove('discarded');
      btn.title = '';
    }} else {{
      discardedSet.add(jid);
      card.classList.add('discarded-card');
      card.style.display = 'none';
      btn.classList.add('discarded');
      btn.title = 'Restore';
    }}
    saveDiscarded();
  }}

  // ── Maybe / Possible jobs ─────────────────────────────────────────────
  const MAYBE_FILE = 'maybe.json';
  let maybeSet = new Set(SERVER_MAYBE);

  async function loadMaybe() {{
    try {{
      const res = await fetch(MAYBE_FILE + '?_=' + Date.now());
      if (res.ok) {{ const arr = await res.json(); arr.forEach(id => maybeSet.add(id)); }}
    }} catch(e) {{}}
    try {{
      const local = JSON.parse(localStorage.getItem('jss_maybe') || '[]');
      local.forEach(id => maybeSet.add(id));
    }} catch(e) {{}}
    initMaybeUI();
  }}

  function initMaybeUI() {{
    document.querySelectorAll('[data-jobid]').forEach(card => {{
      if (maybeSet.has(card.dataset.jobid)) {{
        card.classList.add('maybe-card');
        if (!window._showMaybe) card.style.display = 'none';
        const btn = card.querySelector('.maybe-btn');
        if (btn) {{ btn.classList.add('maybe'); btn.textContent = '↩ Undo Maybe'; }}
      }}
    }});
  }}

  async function saveMaybe() {{
    const arr = Array.from(maybeSet);
    localStorage.setItem('jss_maybe', JSON.stringify(arr));
    const pat  = localStorage.getItem(LS_PAT);
    const user = localStorage.getItem(LS_USER);
    const repo = localStorage.getItem(LS_REPO);
    if (!pat || !user || !repo) return;
    try {{
      const apiUrl = `https://api.github.com/repos/${{user}}/${{repo}}/contents/${{MAYBE_FILE}}`;
      let sha = null;
      try {{ const r = await fetch(apiUrl,{{headers:{{Authorization:`Bearer ${{pat}}`}}}}); if(r.ok){{const d=await r.json();sha=d.sha;}} }} catch(e) {{}}
      const body = {{message:'Update maybe.json', content:btoa(JSON.stringify(arr,null,2))}};
      if (sha) body.sha = sha;
      await fetch(apiUrl,{{method:'PUT',headers:{{Authorization:`Bearer ${{pat}}`,'Content-Type':'application/json'}},body:JSON.stringify(body)}});
    }} catch(e) {{ console.warn('Could not save maybe.json:', e); }}
  }}

  function toggleMaybeCard(btn) {{
    const jid  = btn.dataset.jobid;
    const card = btn.closest('[data-jobid]');
    if (maybeSet.has(jid)) {{
      maybeSet.delete(jid);
      card.classList.remove('maybe-card');
      card.style.display = '';
      btn.classList.remove('maybe');
      btn.textContent = '? Maybe';
    }} else {{
      maybeSet.add(jid);
      card.classList.add('maybe-card');
      card.style.display = 'none';
      btn.classList.add('maybe');
      btn.textContent = '↩ Undo Maybe';
    }}
    saveMaybe();
  }}

  function applyFilters() {{
    const q = document.getElementById('searchBox').value.toLowerCase();
    const minScoreRaw = document.getElementById('minScoreBox').value;
    const minScore = minScoreRaw === '' ? null : parseFloat(minScoreRaw);
    document.querySelectorAll('.card').forEach(card => {{
      const jid      = card.dataset.jobid || '';
      const isApp    = appliedSet.has(jid);
      const isDisc   = discardedSet.has(jid);
      const isMaybe  = maybeSet.has(jid);
      // Discarded and maybe cards are always hidden from the main board
      if (isDisc || isMaybe) {{ card.style.display = 'none'; return; }}
      // Applied cards obey the showApplied toggle independently of filters
      if (isApp) {{
        card.style.display = window._showApplied ? '' : 'none';
        return;
      }}
      const text     = card.innerText.toLowerCase();
      const site     = card.dataset.site || '';
      const isTarget = card.dataset.target === 'true';
      const score    = parseFloat(card.dataset.score || '0');
      const isNew    = card.dataset.new === 'true';
      const matchText   = q === '' || text.includes(q);
      const matchSite   = activeSite === 'all' || site === activeSite;
      const matchTarget = !targetOnly || isTarget;
      const matchScore  = minScore === null || isNaN(minScore) || score >= minScore;
      const matchNew    = !newOnly || isNew;
      card.classList.toggle('hidden', !(matchText && matchSite && matchTarget && matchScore && matchNew));
    }});
  }}

  function filterSite(site, btn) {{
    activeSite = site;
    document.querySelectorAll('.filter-btn').forEach(b => {{
      if (['all','linkedin','indeed'].includes(b.innerText.toLowerCase()) ||
          b.innerText === 'All') b.classList.remove('active');
    }});
    btn.classList.add('active');
    applyFilters();
  }}

  function filterTarget(btn) {{
    targetOnly = !targetOnly;
    btn.classList.toggle('active', targetOnly);
    applyFilters();
  }}

  function filterNew(btn) {{
    newOnly = !newOnly;
    btn.classList.toggle('active', newOnly);
    applyFilters();
  }}

  function sortCards(mode, btn) {{
    activeSort = mode;
    document.querySelectorAll('.sort-label + .filter-btn, .sort-label ~ .filter-btn')
      .forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const grid  = document.getElementById('jobGrid');
    const cards = Array.from(grid.querySelectorAll('.card'));
    cards.sort((a, b) => {{
      if (mode === 'score') {{
        return parseFloat(b.dataset.score || 0) - parseFloat(a.dataset.score || 0);
      }}
      // 'date' sort — rely on DOM order (already sorted by date server-side)
      return 0;
    }});
    cards.forEach(c => grid.appendChild(c));
  }}

  // Run on page load
  loadApplied();
  loadDiscarded();
  loadMaybe();
</script>
</body>
</html>"""

    out_path = os.path.join(OUTPUT_DIR, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n[OK] Site generated: {out_path} ({total} jobs, {target_n} target, {high_fit} high-fit)")

    # Always regenerate archive alongside index
    generate_archive(SCORE_REF)


# ─────────────────────────────────────────────────────────────────
# ARCHIVE PAGE
# ─────────────────────────────────────────────────────────────────

def generate_archive(score_ref: float = 0.0):
    """
    Generate archive.html — shows applied jobs (from localStorage) and
    discarded jobs.

    All jobs are embedded as ARCHIVE_DATA JSON so JS can split them
    into "Applied" vs "Discarded" sections at load time based on localStorage.
    There is no expiry mechanic — jobs stay until the user Discards them.
    """
    updated = datetime.now().strftime("%d %B %Y at %H:%M UTC")

    # Determine score_ref: use passed value, else query DB, else theoretical max
    if score_ref <= 0.0:
        if os.path.exists(DB_PATH):
            try:
                _c = sqlite3.connect(DB_PATH)
                _row = _c.execute(
                    "SELECT MAX(fit_score) FROM jobs WHERE fit_score IS NOT NULL"
                ).fetchone()
                _c.close()
                if _row and _row[0] is not None:
                    score_ref = float(_row[0])
            except Exception:
                pass
        if score_ref <= 0.0:
            score_ref = _MAX_POSSIBLE_SCORE if _MAX_POSSIBLE_SCORE > 0 else 1.0

    # Query ALL accepted jobs from DB (no date filter).
    all_jobs: list[dict] = []
    if os.path.exists(DB_PATH):
        try:
            _conn = sqlite3.connect(DB_PATH)
            rows = _conn.execute(
                """
                SELECT job_id, title, company, location, job_url, site,
                       fit_score, matched_keywords, is_target,
                       MIN(run_date) AS first_seen,
                       MAX(date_posted) AS date_posted,
                       MAX(description) AS description
                FROM   jobs
                WHERE  (rejection_reason IS NULL OR rejection_reason = '')
                GROUP  BY job_id
                ORDER  BY is_target DESC, fit_score DESC
                """
            ).fetchall()
            _conn.close()
            cols = [
                "job_id", "title", "company", "location",
                "job_url", "site", "fit_score", "matched_keywords",
                "is_target", "first_seen", "date_posted", "description",
            ]
            for r in rows:
                row = dict(zip(cols, r))
                # Deserialise matched_keywords robustly
                mk = row.get("matched_keywords") or "[]"
                if isinstance(mk, str):
                    try:
                        kws = json.loads(mk)
                    except Exception:
                        kws = []
                else:
                    kws = list(mk) if mk else []
                kws = [str(k) for k in kws if k]
                if not kws:
                    _title_text = str(row.get("title", "")).lower()
                    _, kws = _compute_fit_score(_title_text)
                # Description truncated for display
                desc_text = str(row.get("description", "") or "").strip()
                desc_display = (desc_text[:200] + "...") if len(desc_text) > 200 else desc_text
                all_jobs.append({
                    "id":          row["job_id"],
                    "title":       row["title"] or "",
                    "company":     row["company"] or "",
                    "location":    row["location"] or "",
                    "url":         row["job_url"] or "#",
                    "site":        (row["site"] or "").lower(),
                    "score":       round(float(row["fit_score"] or 0), 1),
                    "keywords":    kws[:10],
                    "is_target":   bool(row["is_target"]),
                    "date_posted": row["date_posted"] or "",
                    "first_seen":  row["first_seen"] or "",
                    "is_expired":  False,
                    "description": desc_display,
                })
        except Exception as exc:
            print(f"  [archive] Could not query DB: {exc}")

    # Read applied.json so archive page can pre-populate without an extra fetch
    archive_applied: list[str] = []
    applied_json_path = os.path.join(OUTPUT_DIR, "applied.json")
    if os.path.exists(applied_json_path):
        try:
            with open(applied_json_path, "r", encoding="utf-8") as _f:
                archive_applied = json.load(_f)
        except Exception:
            pass

    # Read discarded.json so archive page can pre-populate discarded section
    archive_discarded: list[str] = []
    discarded_json_path = os.path.join(OUTPUT_DIR, "discarded.json")
    if os.path.exists(discarded_json_path):
        try:
            with open(discarded_json_path, "r", encoding="utf-8") as _f:
                archive_discarded = json.load(_f)
        except Exception:
            pass

    # Read maybe.json so archive page can pre-populate maybe section
    archive_maybe: list[str] = []
    maybe_json_path = os.path.join(OUTPUT_DIR, "maybe.json")
    if os.path.exists(maybe_json_path):
        try:
            with open(maybe_json_path, "r", encoding="utf-8") as _f:
                archive_maybe = json.load(_f)
        except Exception:
            pass

    archive_data_json = json.dumps(
        {
            "jobs": all_jobs,
            "score_ref": score_ref,
            "desired_score": DESIRED_SCORE,
            "applied_ids": archive_applied,
            "discarded_ids": archive_discarded,
            "maybe_ids": archive_maybe,
        },
        ensure_ascii=False,
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Archive — {SITE_TITLE}</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #f0f2f5; color: #2c3e50;
}}
header {{
  background: #1a3a5c; color: white; padding: 22px 32px;
  display: flex; justify-content: space-between; align-items: center;
  flex-wrap: wrap; gap: 10px;
}}
header h1 {{ font-size: 21px; font-weight: 600; }}
header p  {{ font-size: 12px; opacity: 0.65; margin-top: 5px; }}
.nav-link {{
  color: rgba(255,255,255,0.85); font-size: 13px; font-weight: 500;
  text-decoration: none; border: 1px solid rgba(255,255,255,0.4);
  padding: 6px 14px; border-radius: 6px; white-space: nowrap;
}}
.nav-link:hover {{ background: rgba(255,255,255,0.15); }}
.section-header {{
  padding: 20px 32px 8px;
  font-size: 16px; font-weight: 700; color: #1a3a5c;
  border-left: 4px solid #2e86ab;
  margin: 24px 32px 0;
  background: white;
  border-radius: 8px 8px 0 0;
  border-top: 1px solid #e8e8e8;
  border-right: 1px solid #e8e8e8;
}}
.grid {{
  padding: 0 32px 32px;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
  gap: 16px;
  background: white;
  border-radius: 0 0 8px 8px;
  border: 1px solid #e8e8e8;
  border-top: none;
  margin: 0 32px 8px;
  padding-top: 16px;
}}
.card {{
  background: #f8fafc; border-radius: 10px; padding: 18px 20px;
  border: 1px solid #e8e8e8; display: flex; flex-direction: column; gap: 8px;
}}
.card:hover {{ box-shadow: 0 4px 18px rgba(0,0,0,0.09); }}
.target-card {{ border-left: 4px solid #f39c12; }}
.applied-card {{ border-left: 4px solid #166534; }}
.card-header {{
  display: flex; justify-content: space-between;
  align-items: flex-start; gap: 8px;
}}
.card-header h3 {{ font-size: 14px; font-weight: 600; line-height: 1.35; }}
.card-header h3 a {{ color: #1a3a5c; text-decoration: none; }}
.card-header h3 a:hover {{ text-decoration: underline; }}
.badges {{ display: flex; flex-direction: column; gap: 3px; flex-shrink: 0; align-items: flex-end; }}
.badge {{
  font-size: 10px; padding: 2px 7px; border-radius: 10px;
  white-space: nowrap; font-weight: 500;
}}
.badge-target   {{ background: #fef3cd; color: #856404; }}
.badge-linkedin {{ background: #e8f4fd; color: #0a66c2; }}
.badge-indeed   {{ background: #e8f8f0; color: #2e7d32; }}
.badge-expired  {{ background: #f3f4f6; color: #6b7280; }}
.badge-applied  {{ background: #dcfce7; color: #166534; font-weight: 700; }}
.badge-desired  {{ background: #fde8e8; color: #c0392b; border: 1px solid #e74c3c; font-weight: 700; }}
.card-meta {{
  display: flex; flex-wrap: wrap; gap: 10px;
  font-size: 12px; color: #666;
}}
.score-row {{
  display: flex; align-items: center; gap: 8px; font-size: 12px;
}}
.score-label {{ color: #888; flex-shrink: 0; }}
.score-num   {{ font-weight: 700; flex-shrink: 0; min-width: 36px; }}
.score-high  {{ color: #1e7e34; }}
.score-mid   {{ color: #856404; }}
.score-low   {{ color: #721c24; }}
.score-bar {{
  flex: 1; height: 6px; background: #e8e8e8;
  border-radius: 4px; overflow: hidden;
}}
.bar-fill {{
  height: 100%; border-radius: 4px;
}}
.bar-fill.score-high {{ background: #28a745; }}
.bar-fill.score-mid  {{ background: #ffc107; }}
.bar-fill.score-low  {{ background: #dc3545; }}
.keywords {{ font-size: 11px; color: #999; }}
.job-desc {{ font-size: 12px; color: #666; margin-top: 6px; line-height: 1.5; }}
.undo-btn {{
  display: inline-block; margin-top: 4px;
  padding: 5px 12px; background: #166534;
  color: white; border-radius: 6px; font-size: 11px;
  border: none; cursor: pointer; font-weight: 500;
}}
.undo-btn:hover {{ background: #14532d; }}
.view-btn {{
  display: inline-block; margin-top: 4px;
  padding: 5px 12px; background: #1a3a5c;
  color: white; border-radius: 6px; font-size: 12px;
  text-decoration: none; font-weight: 500; align-self: flex-start;
}}
.view-btn:hover {{ background: #2e6da4; }}
.restore-btn {{
  display: inline-block; margin-top: 4px;
  padding: 5px 12px; background: #e74c3c;
  color: white; border-radius: 6px; font-size: 11px;
  border: none; cursor: pointer; font-weight: 500;
}}
.restore-btn:hover {{ background: #c0392b; }}
.badge-discarded {{ background: #fde8e8; color: #c0392b; font-weight: 700; }}
.empty-state {{
  padding: 40px 32px; text-align: center; color: #888; font-size: 14px;
  font-style: italic;
}}
.filter-bar {{
  padding: 12px 32px; background: white;
  border-bottom: 1px solid #e8e8e8;
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  position: sticky; top: 0; z-index: 10;
  box-shadow: 0 2px 6px rgba(0,0,0,0.06);
  font-size: 13px;
}}
.filter-bar strong {{ color: #1a3a5c; font-size: 12px; margin-right: 4px; }}
.filter-btn {{
  padding: 4px 13px; border-radius: 20px;
  border: 1px solid #d0d0d0; background: #f5f5f5;
  cursor: pointer; font-size: 12px; font-weight: 500; color: #444;
  transition: background 0.15s, color 0.15s, border-color 0.15s;
}}
.filter-btn.active, .filter-btn:hover {{
  background: #1a3a5c; color: white; border-color: #1a3a5c;
}}
.date-input {{
  padding: 4px 8px; border: 1px solid #d0d0d0;
  border-radius: 6px; font-size: 12px; color: #444;
}}
.filter-sep {{ color: #ccc; font-size: 12px; }}
footer {{
  text-align: center; padding: 24px; font-size: 11px; color: #bbb;
}}
@media (max-width: 600px) {{
  .grid {{ padding: 12px 16px; grid-template-columns: 1fr; margin: 0 16px 8px; }}
  header, .section-header {{ padding-left: 16px; padding-right: 16px; }}
  .section-header {{ margin: 16px 16px 0; }}
}}
</style>
</head>
<body>

<header>
  <div>
    <h1>Archive — {SITE_TITLE}</h1>
    <p>Updated: {updated}</p>
  </div>
  <div style="display:flex; gap:8px; flex-wrap:wrap;">
    <a class="nav-link" href="index.html">Live Job Board</a>
    <a class="nav-link" href="analytics.html">Analytics</a>
    <a class="nav-link" href="settings.html">&#9881; Settings</a>
  </div>
</header>

<div class="filter-bar">
  <strong>Show:</strong>
  <button class="filter-btn active" onclick="setPreset('all', this)">All time</button>
  <button class="filter-btn" onclick="setPreset('7', this)">Last 7 days</button>
  <button class="filter-btn" onclick="setPreset('30', this)">Last 30 days</button>
  <button class="filter-btn" onclick="setPreset('90', this)">Last 90 days</button>
  <span class="filter-sep">&nbsp;|&nbsp;Custom:</span>
  <input type="date" class="date-input" id="dateFrom" onchange="setCustom()">
  <span class="filter-sep">—</span>
  <input type="date" class="date-input" id="dateTo" onchange="setCustom()">
</div>

<div id="appliedSection">
  <div class="section-header">Applied Jobs (<span id="appliedSectionCount">0</span>)</div>
  <div class="grid" id="appliedGrid">
    <div class="empty-state" id="appliedEmpty">No applied jobs yet. Mark jobs as Applied on the Live Job Board.</div>
  </div>
</div>

<div id="maybeSection">
  <div class="section-header">Maybe / Possible (<span id="maybeSectionCount">0</span>)</div>
  <div class="grid" id="maybeGrid">
    <div class="empty-state" id="maybeEmpty">No maybe jobs yet. Mark jobs as Maybe on the Live Job Board.</div>
  </div>
</div>

<div id="expiredSection">
  <div class="section-header">Expired Jobs (<span id="expiredSectionCount">0</span>)</div>
  <div class="grid" id="expiredGrid">
    <div class="empty-state" id="expiredEmpty">No expired jobs yet.</div>
  </div>
</div>

<div id="discardedSection">
  <div class="section-header">Discarded Jobs (<span id="discardedSectionCount">0</span>)</div>
  <div class="grid" id="discardedGrid">
    <div class="empty-state" id="discardedEmpty">No discarded jobs yet.</div>
  </div>
</div>

<footer>
  Auto-generated by job_searcher.py &nbsp;&middot;&nbsp;
  Applied and Discarded state is stored in your browser&apos;s localStorage
</footer>

<script>
const ARCHIVE_DATA = {archive_data_json};
const LS_KEY = 'jss_applied';

// Seed appliedSet from server-embedded applied_ids merged with localStorage
let appliedSet = new Set(ARCHIVE_DATA.applied_ids || []);
try {{
  const local = JSON.parse(localStorage.getItem(LS_KEY) || '[]');
  local.forEach(id => appliedSet.add(id));
}} catch(e) {{}}

// Seed discardedSet from server-embedded discarded_ids merged with localStorage
let discardedSet = new Set(ARCHIVE_DATA.discarded_ids || []);
try {{
  const localDisc = JSON.parse(localStorage.getItem('jss_discarded') || '[]');
  localDisc.forEach(id => discardedSet.add(id));
}} catch(e) {{}}

// Seed maybeSet from server-embedded maybe_ids merged with localStorage
let maybeSet = new Set(ARCHIVE_DATA.maybe_ids || []);
try {{
  const localMaybe = JSON.parse(localStorage.getItem('jss_maybe') || '[]');
  localMaybe.forEach(id => maybeSet.add(id));
}} catch(e) {{}}

function getApplied() {{
  return Array.from(appliedSet);
}}

function saveApplied(arr) {{
  appliedSet = new Set(arr);
  localStorage.setItem(LS_KEY, JSON.stringify(arr));
}}

function saveDiscarded() {{
  localStorage.setItem('jss_discarded', JSON.stringify(Array.from(discardedSet)));
}}

function saveMaybe() {{
  const arr = Array.from(maybeSet);
  localStorage.setItem('jss_maybe', JSON.stringify(arr));
  const pat  = localStorage.getItem('jss_gh_pat');
  const user = localStorage.getItem('jss_gh_user');
  const repo = localStorage.getItem('jss_gh_repo');
  if (!pat || !user || !repo) return;
  const MAYBE_FILE = 'maybe.json';
  try {{
    const apiUrl = `https://api.github.com/repos/${{user}}/${{repo}}/contents/${{MAYBE_FILE}}`;
    let sha = null;
    fetch(apiUrl,{{headers:{{Authorization:`Bearer ${{pat}}`}}}})
      .then(r => r.ok ? r.json() : null)
      .then(d => {{
        if (d && d.sha) sha = d.sha;
        const body = {{message:'Update maybe.json', content:btoa(JSON.stringify(arr,null,2))}};
        if (sha) body.sha = sha;
        return fetch(apiUrl,{{method:'PUT',headers:{{Authorization:`Bearer ${{pat}}`,'Content-Type':'application/json'}},body:JSON.stringify(body)}});
      }})
      .catch(e => console.warn('Could not save maybe.json:', e));
  }} catch(e) {{ console.warn('Could not save maybe.json:', e); }}
}}

function scoreClass(score, ref) {{
  if (score >= 0.6 * ref) return 'score-high';
  if (score >= 0.3 * ref) return 'score-mid';
  return 'score-low';
}}

function buildCard(job, isApplied) {{
  const ref     = ARCHIVE_DATA.score_ref || 1;
  const desired = ARCHIVE_DATA.desired_score || 0;
  const sc      = job.score;
  const cls     = scoreClass(sc, ref);
  const barPct  = Math.min(sc / ref * 100, 100).toFixed(1);
  const kws     = (job.keywords || []).join(', ');
  const dateLbl = job.date_posted ? job.date_posted.slice(0, 10) : (job.first_seen ? job.first_seen.slice(0, 10) : 'Date unknown');
  const siteBadge  = job.site ? `<span class="badge badge-${{job.site}}">${{job.site.toUpperCase()}}</span>` : '';
  const targetBadge = job.is_target ? `<span class="badge badge-target">Target company</span>` : '';
  const desiredBadge = (desired > 0 && sc >= desired) ? `<span class="badge badge-desired">&#9733; Match</span>` : '';
  const statusBadge = isApplied ? `<span class="badge badge-applied">&#10003; Applied</span>` : `<span class="badge badge-expired">Expired</span>`;
  const actionBtn = isApplied
    ? `<button class="undo-btn" onclick="undoApplied('${{job.id}}', this)">&#8617; Undo Applied</button>`
    : '';
  const cardCls = isApplied ? 'card applied-card' : (job.is_target ? 'card target-card' : 'card');

  const descHtml = job.description ? `<div class="job-desc">${{job.description}}</div>` : '';
  const fsDate   = job.first_seen ? job.first_seen.slice(0, 10) : '';
  return `<div class="${{cardCls}}" data-jobid="${{job.id}}" data-firstseen="${{fsDate}}">
  <div class="card-header">
    <h3><a href="${{job.url}}" target="_blank" rel="noopener">${{job.title}}</a></h3>
    <div class="badges">${{desiredBadge}}${{targetBadge}}${{statusBadge}}${{siteBadge}}</div>
  </div>
  <div class="card-meta">
    <span>${{job.company}}</span>
    <span>${{job.location}}</span>
    <span>${{dateLbl}}</span>
  </div>
  <div class="score-row">
    <span class="score-label">Fit:</span>
    <span class="score-num ${{cls}}">${{sc}}</span>
    <div class="score-bar"><div class="bar-fill ${{cls}}" style="width:${{barPct}}%"></div></div>
  </div>
  <div class="keywords">Keywords: ${{kws || '—'}}</div>
  ${{descHtml}}
  <div style="display:flex; gap:8px; align-items:center; margin-top:4px; flex-wrap:wrap;">
    <a class="view-btn" href="${{job.url}}" target="_blank" rel="noopener">View posting</a>
    ${{actionBtn}}
  </div>
</div>`;
}}

function buildDiscardedCard(job) {{
  const ref      = ARCHIVE_DATA.score_ref || 1;
  const desired  = ARCHIVE_DATA.desired_score || 0;
  const sc       = job.score;
  const cls      = scoreClass(sc, ref);
  const barPct   = Math.min(sc / ref * 100, 100).toFixed(1);
  const kws      = (job.keywords || []).join(', ');
  const dateLbl  = job.date_posted ? job.date_posted.slice(0, 10) : (job.first_seen ? job.first_seen.slice(0, 10) : 'Date unknown');
  const siteBadge    = job.site ? `<span class="badge badge-${{job.site}}">${{job.site.toUpperCase()}}</span>` : '';
  const targetBadge  = job.is_target ? `<span class="badge badge-target">Target company</span>` : '';
  const desiredBadge = (desired > 0 && sc >= desired) ? `<span class="badge badge-desired">&#9733; Match</span>` : '';
  const statusBadge  = `<span class="badge badge-discarded">Discarded</span>`;
  const cardCls      = job.is_target ? 'card target-card' : 'card';
  const descHtml2    = job.description ? `<div class="job-desc">${{job.description}}</div>` : '';
  const fsDate2      = job.first_seen ? job.first_seen.slice(0, 10) : '';
  return `<div class="${{cardCls}}" data-jobid="${{job.id}}" data-firstseen="${{fsDate2}}">
  <div class="card-header">
    <h3><a href="${{job.url}}" target="_blank" rel="noopener">${{job.title}}</a></h3>
    <div class="badges">${{desiredBadge}}${{targetBadge}}${{statusBadge}}${{siteBadge}}</div>
  </div>
  <div class="card-meta">
    <span>${{job.company}}</span>
    <span>${{job.location}}</span>
    <span>${{dateLbl}}</span>
  </div>
  <div class="score-row">
    <span class="score-label">Fit:</span>
    <span class="score-num ${{cls}}">${{sc}}</span>
    <div class="score-bar"><div class="bar-fill ${{cls}}" style="width:${{barPct}}%"></div></div>
  </div>
  <div class="keywords">Keywords: ${{kws || '—'}}</div>
  ${{descHtml2}}
  <div style="display:flex; gap:8px; align-items:center; margin-top:4px; flex-wrap:wrap;">
    <a class="view-btn" href="${{job.url}}" target="_blank" rel="noopener">View posting</a>
    <button class="restore-btn" onclick="restoreDiscarded('${{job.id}}', this)">&#8617; Restore</button>
  </div>
</div>`;
}}

function buildMaybeCard(job) {{
  const ref      = ARCHIVE_DATA.score_ref || 1;
  const desired  = ARCHIVE_DATA.desired_score || 0;
  const sc       = job.score;
  const cls      = scoreClass(sc, ref);
  const barPct   = Math.min(sc / ref * 100, 100).toFixed(1);
  const kws      = (job.keywords || []).join(', ');
  const dateLbl  = job.date_posted ? job.date_posted.slice(0, 10) : (job.first_seen ? job.first_seen.slice(0, 10) : 'Date unknown');
  const siteBadge    = job.site ? `<span class="badge badge-${{job.site}}">${{job.site.toUpperCase()}}</span>` : '';
  const targetBadge  = job.is_target ? `<span class="badge badge-target">Target company</span>` : '';
  const desiredBadge = (desired > 0 && sc >= desired) ? `<span class="badge badge-desired">&#9733; Match</span>` : '';
  const statusBadge  = `<span class="badge" style="background:#fef3cd;color:#92400e;font-weight:700;">? Maybe</span>`;
  const cardCls      = job.is_target ? 'card target-card' : 'card';
  const descHtml3    = job.description ? `<div class="job-desc">${{job.description}}</div>` : '';
  const fsDate3      = job.first_seen ? job.first_seen.slice(0, 10) : '';
  return `<div class="${{cardCls}}" data-jobid="${{job.id}}" data-firstseen="${{fsDate3}}">
  <div class="card-header">
    <h3><a href="${{job.url}}" target="_blank" rel="noopener">${{job.title}}</a></h3>
    <div class="badges">${{desiredBadge}}${{targetBadge}}${{statusBadge}}${{siteBadge}}</div>
  </div>
  <div class="card-meta">
    <span>${{job.company}}</span>
    <span>${{job.location}}</span>
    <span>${{dateLbl}}</span>
  </div>
  <div class="score-row">
    <span class="score-label">Fit:</span>
    <span class="score-num ${{cls}}">${{sc}}</span>
    <div class="score-bar"><div class="bar-fill ${{cls}}" style="width:${{barPct}}%"></div></div>
  </div>
  <div class="keywords">Keywords: ${{kws || '—'}}</div>
  ${{descHtml3}}
  <div style="display:flex; gap:8px; align-items:center; margin-top:4px; flex-wrap:wrap;">
    <a class="view-btn" href="${{job.url}}" target="_blank" rel="noopener">View posting</a>
    <button class="restore-btn" onclick="restoreMaybe('${{job.id}}', this)" style="background:#f39c12;">&#8617; Restore</button>
  </div>
</div>`;
}}

function undoApplied(jid, btn) {{
  appliedSet.delete(jid);
  saveApplied(Array.from(appliedSet));
  // Remove card from applied section
  const card = btn.closest('[data-jobid]');
  if (card) card.remove();
  // Move to expired section if job is expired and not discarded
  const job = (ARCHIVE_DATA.jobs || []).find(j => j.id === jid);
  if (job && job.is_expired && !discardedSet.has(jid)) {{
    const expiredGrid = document.getElementById('expiredGrid');
    const emptyEl = document.getElementById('expiredEmpty');
    if (emptyEl) emptyEl.remove();
    expiredGrid.insertAdjacentHTML('beforeend', buildCard(job, false));
  }}
  updateCounts();
}}

function restoreDiscarded(jid, btn) {{
  discardedSet.delete(jid);
  saveDiscarded();
  // Remove card from discarded section
  const card = btn.closest('[data-jobid]');
  if (card) card.remove();
  // Move to expired section
  const job = (ARCHIVE_DATA.jobs || []).find(j => j.id === jid);
  if (job && job.is_expired) {{
    const expiredGrid = document.getElementById('expiredGrid');
    const emptyEl = document.getElementById('expiredEmpty');
    if (emptyEl) emptyEl.remove();
    expiredGrid.insertAdjacentHTML('beforeend', buildCard(job, false));
  }}
  updateCounts();
}}

function restoreMaybe(jid, btn) {{
  maybeSet.delete(jid);
  saveMaybe();
  // Remove card from maybe section
  const card = btn.closest('[data-jobid]');
  if (card) card.remove();
  // Move to expired section
  const job = (ARCHIVE_DATA.jobs || []).find(j => j.id === jid);
  if (job && job.is_expired) {{
    const expiredGrid = document.getElementById('expiredGrid');
    const emptyEl = document.getElementById('expiredEmpty');
    if (emptyEl) emptyEl.remove();
    expiredGrid.insertAdjacentHTML('beforeend', buildCard(job, false));
  }}
  updateCounts();
}}

function countVisible(gridEl) {{
  return Array.from(gridEl.querySelectorAll('[data-jobid]'))
    .filter(c => c.style.display !== 'none').length;
}}

function updateCounts() {{
  const appliedGrid    = document.getElementById('appliedGrid');
  const maybeGrid      = document.getElementById('maybeGrid');
  const expiredGrid    = document.getElementById('expiredGrid');
  const discardedGrid  = document.getElementById('discardedGrid');
  const appliedCards   = countVisible(appliedGrid);
  const maybeCards     = countVisible(maybeGrid);
  const expiredCards   = countVisible(expiredGrid);
  const discardedCards = countVisible(discardedGrid);
  document.getElementById('appliedSectionCount').textContent = appliedCards;
  document.getElementById('maybeSectionCount').textContent = maybeCards;
  document.getElementById('expiredSectionCount').textContent = expiredCards;
  document.getElementById('discardedSectionCount').textContent = discardedCards;
  if (appliedCards === 0 && !document.getElementById('appliedEmpty')) {{
    appliedGrid.innerHTML = '<div class="empty-state" id="appliedEmpty">No applied jobs yet. Mark jobs as Applied on the Live Job Board.</div>';
  }}
  if (maybeCards === 0 && !document.getElementById('maybeEmpty')) {{
    maybeGrid.innerHTML = '<div class="empty-state" id="maybeEmpty">No maybe jobs yet. Mark jobs as Maybe on the Live Job Board.</div>';
  }}
  if (expiredCards === 0 && !document.getElementById('expiredEmpty')) {{
    expiredGrid.innerHTML = '<div class="empty-state" id="expiredEmpty">No expired jobs yet.</div>';
  }}
  if (discardedCards === 0 && !document.getElementById('discardedEmpty')) {{
    discardedGrid.innerHTML = '<div class="empty-state" id="discardedEmpty">No discarded jobs yet.</div>';
  }}
}}

// ── Date filter ──────────────────────────────────────────────────
let _filterFrom = null;  // Date | null
let _filterTo   = null;  // Date | null

function setPreset(days, btn) {{
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('dateFrom').value = '';
  document.getElementById('dateTo').value   = '';
  if (days === 'all') {{
    _filterFrom = null;
    _filterTo   = null;
  }} else {{
    _filterTo   = new Date();
    _filterFrom = new Date();
    _filterFrom.setDate(_filterFrom.getDate() - parseInt(days));
  }}
  applyDateFilter();
}}

function setCustom() {{
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  const fv = document.getElementById('dateFrom').value;
  const tv = document.getElementById('dateTo').value;
  _filterFrom = fv ? new Date(fv) : null;
  _filterTo   = tv ? new Date(tv) : null;
  applyDateFilter();
}}

function _inRange(dateStr) {{
  if (!_filterFrom && !_filterTo) return true;
  if (!dateStr) return true;   // no date → always show
  const d = new Date(dateStr);
  if (_filterFrom && d < _filterFrom) return false;
  if (_filterTo) {{
    const end = new Date(_filterTo);
    end.setHours(23, 59, 59, 999);
    if (d > end) return false;
  }}
  return true;
}}

function applyDateFilter() {{
  ['appliedGrid','maybeGrid','expiredGrid','discardedGrid'].forEach(id => {{
    document.getElementById(id).querySelectorAll('[data-jobid]').forEach(card => {{
      card.style.display = _inRange(card.getAttribute('data-firstseen')) ? '' : 'none';
    }});
  }});
  updateCounts();
}}

// ── Boot ─────────────────────────────────────────────────────────
(function init() {{
  const jobs          = ARCHIVE_DATA.jobs || [];
  const appliedGrid   = document.getElementById('appliedGrid');
  const maybeGrid     = document.getElementById('maybeGrid');
  const expiredGrid   = document.getElementById('expiredGrid');
  const discardedGrid = document.getElementById('discardedGrid');

  let appliedHtml   = '';
  let maybeHtml     = '';
  let expiredHtml   = '';
  let discardedHtml = '';

  for (const job of jobs) {{
    const isApp   = appliedSet.has(job.id);
    const isMaybe = maybeSet.has(job.id);
    const isDisc  = discardedSet.has(job.id);
    if (isApp) {{
      appliedHtml += buildCard(job, true);
    }} else if (isMaybe) {{
      maybeHtml += buildMaybeCard(job);
    }} else if (isDisc) {{
      discardedHtml += buildDiscardedCard(job);
    }} else if (job.is_expired) {{
      expiredHtml += buildCard(job, false);
    }}
  }}

  if (appliedHtml)   {{ appliedGrid.innerHTML   = appliedHtml;   }}
  if (maybeHtml)     {{ maybeGrid.innerHTML     = maybeHtml;     }}
  if (expiredHtml)   {{ expiredGrid.innerHTML   = expiredHtml;   }}
  if (discardedHtml) {{ discardedGrid.innerHTML = discardedHtml; }}

  updateCounts();
}})();
</script>
</body>
</html>"""

    out_path = os.path.join(OUTPUT_DIR, "archive.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Archive page generated: {out_path} ({len(all_jobs)} total jobs embedded)")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def run_search(save: bool = False, make_site: bool = False):
    run_dt = datetime.now()
    print(f"\n[{run_dt.strftime('%Y-%m-%d %H:%M')}] Starting job search...")

    raw_df      = scrape_all()
    filtered_df = filter_jobs(raw_df)
    print_results(filtered_df)

    # Build set of accepted job IDs for the DB writer
    accepted_ids: set[str] = set()
    if not filtered_df.empty:
        for _, row in filtered_df.iterrows():
            title   = str(row.get("title",   "") or "")
            company = str(row.get("company", "") or "")
            url     = str(row.get("job_url", "") or "")
            accepted_ids.add(_job_id(title, company, url))

    # Always persist to DB (regardless of --site / --save flags)
    save_to_db(filtered_df, raw_df, accepted_ids, run_dt)

    if save:
        save_results(filtered_df)
    if make_site:
        generate_site(filtered_df)
        generate_analytics()


def run_daily():
    """Run once immediately, then schedule daily at 08:00 local time.
    NOTE: If you are using GitHub Actions, you do not need --daily.
    The workflow already handles scheduling via cron.
    """
    print("Daily scheduler started. Runs every day at 08:00 local time.")
    print("Press Ctrl+C to stop.\n")
    run_search(save=True, make_site=True)
    schedule.every().day.at("08:00").do(
        run_search, save=True, make_site=True
    )
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Job Searcher — Indeed + LinkedIn"
    )
    parser.add_argument("--save",      action="store_true", help="Save results to CSV")
    parser.add_argument("--site",      action="store_true", help="Generate index.html + analytics.html for GitHub Pages")
    parser.add_argument("--daily",     action="store_true", help="Run now + schedule daily at 08:00")
    parser.add_argument("--analytics", action="store_true", help="Regenerate analytics.html from existing jobs.db (no scrape)")
    args = parser.parse_args()

    if args.analytics:
        generate_analytics()
    elif args.daily:
        run_daily()
    else:
        run_search(save=args.save, make_site=args.site)
