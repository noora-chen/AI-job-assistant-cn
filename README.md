# Job Searcher — Automated Job Board with GitHub Pages

Scrapes Indeed and LinkedIn daily, scores jobs by keyword relevance, and publishes a live dashboard to GitHub Pages — fully automated, zero server needed.

> Built for control systems / robotics roles in the Netherlands, but fully customisable for **any profession and any country** via the Settings page.

---

## Live demo

Once deployed, your dashboard looks like this:

| Page | What it shows |
|---|---|
| `index.html` | Ranked job cards, colour-coded new vs returning, fit score bar, 🔥 top-match banner |
| `analytics.html` | Weekly **or daily** trends, new-job curve, target-company chart, desired-match history |
| `settings.html` | Edit every filter, keyword, weight and search parameter in the browser — no code needed |

---

## Deploy your own copy — step by step

### 1. Fork or copy this repo

Click **Fork** (top-right on GitHub) to get your own copy, **or**:

```
git clone https://github.com/ORIGINAL_OWNER/REPO_NAME.git my-job-searcher
cd my-job-searcher
git remote set-url origin https://github.com/YOUR_USERNAME/YOUR_NEW_REPO.git
git push -u origin main
```

---

### 2. Enable GitHub Pages

1. Go to your repo → **Settings** → **Pages**
2. Under *Source*, choose **Deploy from a branch**
3. Branch: **`gh-pages`** / folder: `/ (root)`
4. Click **Save**

> The `gh-pages` branch is created automatically the first time the workflow runs. If Pages shows an error before that, just wait for the first run to finish.

---

### 3. Run the workflow for the first time

1. Go to your repo → **Actions**
2. Click **Daily Job Search** in the left sidebar
3. Click **Run workflow** → **Run workflow** (green button)

This takes ~1–2 minutes. When it completes, your Pages URL (`https://YOUR_USERNAME.github.io/YOUR_REPO/`) will show live job results.

The workflow also runs automatically every day at **09:00 CET/CEST (07:00 UTC)** and whenever you push a change to `main`. To change the time, edit the `cron:` line in `.github/workflows/job_search.yml`.

---

### 4. Customise for your profession

Open `https://YOUR_USERNAME.github.io/YOUR_REPO/settings.html` in your browser.

**First thing to change:**

| Field | Example |
|---|---|
| Site title | `Software Jobs NL`, `Mechanical Engineering NL`, `Finance Roles UK` |
| Country | `Netherlands`, `Germany`, `United Kingdom`, `USA` |
| Search queries | `software engineer`, `data scientist`, `mechanical design engineer` |

Then edit the keyword tiers to match your field. Click **Save to GitHub** — the next workflow run picks up your new config automatically.

> You need a GitHub Personal Access Token (PAT) to save from the browser. See step 5.

---

### 5. Create a GitHub Personal Access Token (PAT)

The Settings page saves `config.json` back to your repo via the GitHub API. For this it needs a PAT.

1. Go to [github.com/settings/tokens](https://github.com/settings/tokens)
2. Click **Generate new token (classic)**
3. Give it a name, e.g. `job-searcher-settings`
4. Set expiration (90 days is fine; you can regenerate later)
5. Tick the **`repo`** scope (full control of private repositories)
6. Click **Generate token** and **copy it immediately** — you won't see it again

Back on your Settings page (`settings.html`):
- Paste your GitHub username, repo name (`YOUR_USERNAME/YOUR_REPO`), and PAT into the **GitHub connection** fields at the top
- These are stored only in your browser (localStorage) — they are never committed to the repo

---

## How it works

```
GitHub Actions (daily cron)
  └─ job_searcher.py --site --save
       ├─ Scrapes Indeed + LinkedIn
       ├─ Filters jobs through the pipeline
       ├─ Scores each job by keyword depth
       ├─ Writes results to jobs.db (persisted on a separate data-branch)
       ├─ Generates index.html, analytics.html, settings.html
       └─ Deploys to gh-pages branch → GitHub Pages serves it live
```

---

## Filter pipeline

Jobs pass through four stages in order. All parameters are editable in `settings.html`.

| Stage | What it does |
|---|---|
| **Seniority filter** | Removes "senior", "lead", "principal", "manager", "5+ years", etc. from titles |
| **Title blacklist** | Removes jobs whose title contains unwanted terms (e.g. PLC, SCADA, HMI) |
| **Full-text blacklist** | Removes jobs mentioning unwanted phrases anywhere in the description |
| **Keyword gate** | Job must match at least one high-weight keyword to pass |

---

## Fit score

Each job is scored by **weighted keyword depth**:

```
score = Σ (weight × number of unique keywords matched in that tier)
```

Tiers, weights, and keywords are fully configurable in Settings. The default setup has three tiers (high / mid / low relevance). Jobs are sorted by score descending; target companies are always pinned to the top.

---

## Analytics

The analytics dashboard tracks your job market over time. Key behaviours:

- **Each job is counted only once** — if the same listing reappears across multiple daily runs it is counted on the day it was *first seen*, never double-counted.
- **Weekly / Daily toggle** — switch all charts between weekly aggregation (default) and day-by-day view with a single click. The range selector adapts automatically (default: 8 weeks or last 30 days).
- Charts covered: accepted jobs, new jobs, desired matches, target-company appearances, source split (Indeed vs LinkedIn), average fit score.

---

## Top-match notifications (desired fit score)

Set a **desired fit score** threshold in Settings. When the daily run finds new jobs at or above that score:

- Each matching card gets a red **★ Match** badge
- A 🔥 banner at the top of the dashboard shows the count — e.g. *"🔥 3 top matches today (score ≥ 200)"*
- Analytics tracks desired matches per week on a dedicated chart and stat card

Set `desired_score` to `0` (default) to disable the feature entirely.

---

## Settings reference

All settings live in `config.json` (edited via `settings.html`).

| Setting | Purpose |
|---|---|
| `site_title` | Page title shown in the browser tab and header |
| `country` | Single location (city/country) passed to both Indeed and LinkedIn. Kept for backward compatibility. |
| `locations` | **New.** A list of cities to search across, e.g. `["Hangzhou, China", "Chengdu, China"]`. If present, this overrides `country` — the script loops every search query over every city in this list. |
| `country_indeed` | **New.** The country name passed to Indeed's `country_indeed` filter (Indeed wants a country, not a city). Defaults to `"China"` if omitted. |
| `search_queries` | Search terms sent to Indeed + LinkedIn |
| `scored_keywords` | Tiered keyword groups with weights — core of the scoring |
| `blacklist_title` | Terms that disqualify a job if found in the title |
| `blacklist_full` | Terms that disqualify a job if found anywhere in the text |
| `senior_patterns` | Regex patterns for seniority signals in the title |
| `target_companies` | Pinned to top of results and marked with `★` |
| `results_per_query` | Results fetched per search term per site (default 30) |
| `hours_old` | Only include jobs posted within this many hours (default 168 = 7 days) |
| `min_keywords` | Minimum total keyword matches required (default 3) |
| `desired_score` | Highlight jobs at or above this score with a red ★ Match badge and daily banner; `0` = disabled |

---

## Local usage (optional — not required)

> **You don't need this.** The tool runs fully automatically via GitHub Actions — just fork, run the workflow once, and you're done. Local usage is only useful if you want to test changes before pushing, or if you prefer to run it without GitHub.

If you do want to run it on your own machine:

```bash
pip install python-jobspy pandas schedule
```

| Command | What it does |
|---|---|
| `python job_searcher.py` | Search now, print results to terminal |
| `python job_searcher.py --save` | Search + save results to CSV |
| `python job_searcher.py --site` | Search + generate HTML dashboard |
| `python job_searcher.py --site --save` | Full run (same as GitHub Actions) |
| `python job_searcher.py --daily` | Run now + repeat daily at 08:00 |

> **Windows note:** if `pip install python-jobspy` fails with a numpy compile error, run `conda install numpy pandas -y` first (if you have Anaconda/Miniconda), then `pip install python-jobspy --no-deps`.

---

## Resetting the job history

To clear the database and start fresh (all jobs will appear as "new"):

1. Go to your repo → **Settings** → **Branches** → find `data-branch` → delete it

The next workflow run creates a new empty `jobs.db`.

---

## Full reset (keep your settings)

To reset everything — job history, applied/discarded/maybe lists — while keeping your `config.json` intact:

**Step 1 — Delete the data branch** (clears `jobs.db`):

```bash
git push origin --delete data-branch
```

Or via GitHub: repo → **Settings** → **Branches** → delete `data-branch`.

**Step 2 — Reset the state files** (clears Applied / Maybe / Discarded):

```bash
echo '[]' > applied.json
echo '[]' > discarded.json
echo '[]' > maybe.json
git add applied.json discarded.json maybe.json
git commit -m "Reset state files"
git push
```

**Step 3 — Re-run the workflow**:

Go to **Actions** → **Daily Job Search** → **Run workflow**.

The next run starts with a fresh database. All jobs will appear as new, and your applied/discarded/maybe lists will be empty.

> **What's preserved:** everything in `config.json` — keywords, weights, search queries, filters, and all other settings.
>
> **What's gone:** all job history, seen/new tracking, and applied/discarded/maybe lists.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Pages shows the README instead of the dashboard | The workflow hasn't run yet. Go to Actions → run it manually once. |
| "gh-pages branch not found" error in Pages settings | Same — run the workflow first, then set the Pages source. |
| Settings page says "Failed to save" | Check your PAT has the `repo` scope and hasn't expired. |
| No jobs appear | Your keyword gate may be too strict, or the search query returns nothing. Lower `min_keywords` to 1 in Settings and check the terminal output locally. |
| Workflow fails on `pip install` | See the Windows numpy note above, or check the Actions log for the exact error. |
