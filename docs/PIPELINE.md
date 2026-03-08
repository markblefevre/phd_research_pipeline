# Pipeline Documentation (Living)

Last updated: 2026-03-08  
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

All *run-scoped* stages in the pipeline share the same `run_id`.

### Standard output locations

- Run artifacts:
  - `outputs/<paper>/<run_id>/...`

- Run metadata:
  - `runs/<paper>/<run_id>/...`

### Curated artifacts (not run-scoped)

Curated artifacts are written to `data/curated/<paper>/...` and are reused across runs unless explicitly regenerated.

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
- `runs/paper1/<run_id>/command.txt`
- `runs/paper1/<run_id>/config_resolved.json`
- Stage-specific outputs (see stage docs below)

---

## Stage Inventory

Legend:
- ✅ migrated: uses new repo paths + `paper/run_id` output convention
- 🟡 hybrid: may read legacy data paths but writes new outputs
- ❌ legacy: still uses old `Code/out/...` assumptions

| Stage ID | Name | Status | Entrypoint (module) | Notes |
|---|---|---:|---|---|
| S0 | Build Panel: merge GPT + LMMD | ✅ | `src/panel/run_build_panel.py` | Produces curated panel used by downstream stages; writes QC sidecar |
| S0.5 | Market Model: estimate alphas/betas | ✅ | `src/event_study/run_market_model_stage.py` | Produces curated `alphas_betas.csv`; configurable basis; not run-scoped |
| S1 | Event Study: CAR compute + baseline regressions | ✅ | `src/event_study/run_event_study_all.py` | Reads curated inputs; writes run-scoped outputs |
| S2 | Event Study: Horse-race regression grid | ✅ | `src/event_study/run_event_study_horserace.py` | Reads S1 outputs; writes run-scoped summary |

---

## Stage Execution Order

Default order (Paper 1):

1. S0 `build_panel`
2. S0.5 `market_model`
3. S1 `event_study`
4. S2 `horserace`

---

## Stage Contracts

### S0 — Build Panel: Merge GPT + LMMD

**Stage ID:** S0  
**Module:** `src/panel/run_build_panel.py`  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `build_panel = true`)

#### Purpose
Create an event-level panel containing both:
- GPT sentiment (`document_score` + chunk summaries)
- LMMD sentiment (`lmmd_net`, `pos_rate`, `neg_rate`)

This panel is the canonical input to S1 and S2.

#### Inputs (curated)
Configured in `pipeline.toml`:

- GPT panel (filtered):
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_filtered.csv`

- LMMD scores:
  - `data/curated/<paper>/panel/lmmd_scores_nikkei225.csv`
  - Note: LMMD may store the date as `filing_date_parsed`; stage normalizes to `filing_date`.

#### Parameters
- `how`: merge type (default: `inner`)
- `dropna_cols`: columns required to be non-missing (default: `document_score`, `lmmd_net`, `pos_rate`, `neg_rate`)

#### Outputs (curated)
- Panel:
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv`

- QC sidecar:
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv.qc.json`
  - Contains row counts, unique key counts, merged row counts pre/post dropna, etc.

#### Skip condition (pipeline)
If `skip_if_exists = true`, stage is skipped when:
- `out_csv` exists

#### Notes
- Output is intentionally kept **legacy-compatible** in schema (merge suffixing and post-merge column pruning), to minimize downstream breakage.

---

### S0.5 — Market Model: Estimate alphas/betas

**Stage ID:** S0.5  
**Module:** `src/event_study/run_market_model_stage.py`  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `market_model = true`)

#### Purpose
Estimate per-ticker market model parameters (alpha, beta) versus TOPIX:
- Used by S1 to compute abnormal returns for CARs.

#### Inputs (curated)
Configured in `pipeline.toml`:

- Market index prices (TOPIX):
  - `data/curated/<paper>/prices/TOPIX_prices.csv`

- Stock prices (long format):
  - `data/curated/<paper>/prices/prices_long.csv`

#### Parameters
- `basis`:
  - `price` (use close) or `total` (use adj_close / total return proxy)
- Calendar handling:
  - stage enforces overlapping date intersection between market + stocks (to avoid misalignment)

#### Outputs (curated)
- `data/curated/<paper>/event_study/alphas_betas.csv`  
  Columns: `Ticker, alpha, beta`

#### Skip condition (pipeline)
If `skip_if_exists = true`, stage is skipped when:
- `alphas_betas.csv` exists

#### Notes
- Output is curated (not run-scoped). If you change `basis` or input files, delete the output (or disable skip) to regenerate.

---

### S1 — Event Study: CAR compute + baseline regressions

**Stage ID:** S1  
**Module:** `src/event_study/run_event_study_all.py`  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `event_study = true`)

#### Purpose
For each event window and sentiment measure:
- compute CAR per (Ticker, EventDate)
- save per-window CAR dataset
- run baseline clustered regression and save regression summary

#### Inputs (curated)
Configured in `pipeline.toml`:

- Event-level sentiment panel:
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv`

- Market model params:
  - `data/curated/<paper>/event_study/alphas_betas.csv`

- Stock prices:
  - `data/curated/<paper>/prices/prices_long.csv`

- Market index prices:
  - `data/curated/<paper>/prices/TOPIX_prices.csv`

#### Parameters
- `sentiment_cols`: e.g., `document_score`, `lmmd_net`, `neg_rate`, `pos_rate`
- `windows`: e.g., `[(0,0), (0,1), (-1,1), (-2,2), (-3,3)]`
- `paper`, `run_id`: used for output routing

#### Outputs (run-scoped)
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

---

### S2 — Event Study: Horse-race regression grid (GPT vs LMMD)

**Stage ID:** S2  
**Module:** `src/event_study/run_event_study_horserace.py`  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `horserace = true`)

#### Purpose
For each window:
- load CARs from S1 output (document_score CARs as dependent variable source)
- merge CARs with multiple sentiment measures
- run a grid of regression specifications:
  - GPT only
  - LMMD only
  - GPT + LMMD (horse-race)
  - LMMD components (pos/neg) diagnostics
- write one combined summary table

#### Inputs
Configured in `pipeline.toml`:

- Panel (curated):
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv`

- CAR files from S1 (run-scoped):
  - `outputs/<paper>/<run_id>/event_study/car_results_all_{w0}_{w1}_document_score.csv`

#### Outputs (run-scoped)
- Summary table:
  - `outputs/<paper>/<run_id>/event_study/horserace_summary.csv`

#### Skip condition (pipeline)
If `skip_if_exists = true`, stage is skipped when:
- `outputs/<paper>/<run_id>/event_study/horserace_summary.csv` exists

#### Notes
- Stage z-scores sentiment measures in-sample for comparability across measures/specs.
- CAR is treated as sentiment-invariant and used as the dependent variable.

---

## Migration Plan (Near-term)

### Next candidate stages (future)
Likely additions (not yet implemented here):
- Fetch/refresh raw EDINET filings
- Extract MD&A text (taxonomy-based)
- Run GPT sentiment jobs (expensive, tokenized)
- Compute LMMD scores from extracted text
- Produce filtered GPT panel (pre-merge input to S0)

Goal: bring all upstream steps into staged, config-driven pipeline with clear curated outputs.

---

## Glossary / Notes

- **CAR**: cumulative abnormal return over an event window (e.g., [-1,1])
- **EventDate**: filing date aligned to a trading day
- **LMMD**: lexicon-based tone measures (net, pos_rate, neg_rate)
- **GPT sentiment**: LLM-derived `document_score` (or equivalent)
- **run_id**: ties all run-scoped artifacts for one pipeline execution together
- **curated**: stable, analysis-ready datasets reused across runs

---

## Changelog

- 2026-03-07: Added pipeline skeleton + stage contracts for S1/S2; initial migration notes.
- 2026-03-08: Added S0 (build_panel) + S0.5 (market_model) as curated stages; migrated S2 outputs to run-scoped path; added QC sidecar for panel merge; updated all stage inputs to curated paths.
