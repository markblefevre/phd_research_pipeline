# Pipeline Documentation (Living)

Last updated: 2026-03-07  
Owner: Mark Lefevre

## Purpose

This document is the living specification for the research pipeline (Paper 1 / Paper 2).  
It defines:

- Stage inventory (what exists, what is migrated vs legacy)
- The input/output “contracts” for each stage
- Run conventions (paper, run_id, output locations)
- Migration notes (what changed, what still uses legacy paths)

---

## Repo Conventions

### Directory conventions

- `src/`  
  Reusable library code (importable modules). No side-effect execution.

- `scripts/`  
  Entrypoints (“do the thing”). Thin orchestration only.

- `configs/`  
  Run configuration files (TOML). Paper-scoped + shared.

- `data/`
  - `data/raw/` = source data (inputs)
  - `data/interim/` = intermediate artifacts (cacheable)
  - `data/curated/` = analysis-ready tables (cacheable)

- `outputs/`  
  Run artifacts (tables/figures/logs/regression dumps). Safe to delete and recreate.

- `runs/`  
  Run metadata (reproducibility): config snapshot, git commit, command line.

- `papers/`  
  LaTeX/figures/tables used for the actual paper PDFs.

### Run identity

A single pipeline execution is identified by:

- `paper`: `paper1` or `paper2`
- `run_id`: timestamp string, e.g. `2026-03-07_044833`

All stages in the pipeline should share the same `run_id`.

### Standard output locations

- Run artifacts:
  - `outputs/<paper>/<run_id>/...`

- Run metadata:
  - `runs/<paper>/<run_id>/...`

---

## How to Run (Current)

### Pipeline driver (Paper 1)

- Config:
  - `configs/paper1/pipeline.toml`

- Entrypoint:
  - `scripts/paper1/run_pipeline.py`

Example:
```bash
python scripts/paper1/run_pipeline.py
```

Expected:
- `outputs/paper1/<run_id>/logs/run.log`
- `runs/paper1/<run_id>/git.txt`
- Stage-specific outputs (see stage docs below)

---

## Stage Inventory

Legend:
- ✅ migrated: uses new repo paths + `paper/run_id` output convention
- 🟡 hybrid: may read legacy data paths but writes new outputs
- ❌ legacy: still uses old `Code/out/...` assumptions

| Stage ID | Name | Status | Entrypoint | Notes |
|---|---|---:|---|---|
| S1 | Event Study: CAR compute + baseline regressions | 🟡 | `src/event_study/run_event_study_all.py` | Can read legacy inputs; writes to `outputs/<paper>/<run_id>/event_study/` |
| S2 | Event Study: Horse-race regression grid (GPT vs LMMD) | ❌ | `src/event_study/run_event_study_horserace.py` | Still reads/writes `Code/out/event_study`; to be migrated next |

---

## Stage Contracts

### S1 — Event Study: CAR compute + baseline regressions

**Stage ID:** S1  
**Module:** `src/event_study/run_event_study_all.py`  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `event_study = true`)

#### Purpose
For each event window:
- compute CAR per (Ticker, EventDate)
- save a per-window CAR dataset
- run a simple clustered regression and save a regression summary

#### Inputs (current)
- Event-level sentiment panel:
  - `Code/out/mdna_summary_nikkei225_with_lmmd.csv` (legacy location)
- Market model params:
  - `Code/out/alphas_betas.csv` (legacy location)
- Stock prices:
  - `nikkei/out/prices_long.csv` (legacy location)
- Market index prices:
  - `nikkei/out/TOPIX_prices.csv` (legacy location)

#### Parameters
- `sentiment_col`: e.g., `document_score`, `lmmd_net`, `neg_rate`, `pos_rate`
- `windows`: e.g., `[(0,0), (0,1), (-1,1), (-2,2), (-3,3)]`
- `paper`, `run_id`: used for output routing

#### Outputs (current behavior)
Directory:
- `outputs/<paper>/<run_id>/event_study/`

Files:
- Per-window CAR results:
  - `car_results_all_{w0}_{w1}_{sentiment_col}.csv`
- Regression summary (one per sentiment_col):
  - `regression_summary_{sentiment_col}.csv`

#### Skip condition (pipeline)
Stage is considered “done” for a given `sentiment_col` if:
- `outputs/<paper>/<run_id>/event_study/regression_summary_{sentiment_col}.csv` exists

#### Notes
- S1 is “hybrid” while it still reads inputs from legacy locations.
- Long-term goal: read analysis inputs from `data/curated/<paper>/...`.

---

### S2 — Event Study: Horse-race regression grid (GPT vs LMMD)

**Stage ID:** S2  
**Module:** `src/event_study/run_event_study_horserace.py`

#### Purpose
For each window:
- load CARs from S1 output
- merge CARs with multiple sentiment measures
- run a grid of regression specifications:
  - GPT only
  - LMMD only
  - GPT + LMMD (horse-race)
  - LMMD components (pos/neg) diagnostics
- optional Year FE / Industry FE toggles
- write one combined summary table

#### Inputs (current legacy behavior)
- Event-level sentiment panel:
  - `Code/out/mdna_summary_nikkei225_with_lmmd.csv`
- CAR files from S1 (expects document_score files):
  - `Code/out/event_study/car_results_all_{w0}_{w1}_document_score.csv`
- Industry mapping dependency (via `attach_ticker_industry(...)`):
  - (mapping source TBD; document once confirmed)

#### Outputs (current legacy behavior)
- Summary table:
  - `Code/out/event_study/horserace_summary.csv`

#### Planned migration target
Inputs (new):
- CARs from:
  - `outputs/<paper>/<run_id>/event_study/car_results_all_{w0}_{w1}_document_score.csv`
(or long-term: `data/curated/<paper>/event_study/...`)

Outputs (new):
- `outputs/<paper>/<run_id>/event_study/horserace_summary.csv`

#### Notes
- This stage z-scores sentiment measures in-sample for comparability across measures/specs.
- CAR is treated as sentiment-invariant and used as the dependent variable.

---

## Migration Plan (Near-term)

### Next to migrate: S2 (Horse-race)
Goals:
- accept `paper` and `run_id` (like S1)
- read CARs from `outputs/<paper>/<run_id>/event_study/`
- write summary to `outputs/<paper>/<run_id>/event_study/`
- ensure imports are package-safe (Mac + Windows)
- add skip condition (summary file exists)

---

## Glossary / Notes

- **CAR**: cumulative abnormal return over an event window (e.g., [-1,1])
- **EventDate**: filing date aligned to a trading day
- **LMMD**: lexicon-based tone measures (net, pos_rate, neg_rate)
- **GPT sentiment**: LLM-derived `document_score` (or equivalent)
- **run_id**: ties all artifacts for one pipeline execution together

---

## Changelog (optional)

- 2026-03-07: Added pipeline skeleton + stage contracts for S1 and S2; S1 writes to `outputs/<paper>/<run_id>/...`; S2 still legacy.
