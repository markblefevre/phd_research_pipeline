# Pipeline Documentation (Living)

Last updated: 2026-03-09  
Owner: Mark Lefevre

## Purpose

This document is the living specification for the research pipeline (Paper 1 / Paper 2).
It defines:

- Stage inventory (what exists, what is migrated vs legacy)
- The input/output “contracts” for each stage
- Run conventions (`paper`, `run_id`, output locations)
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
  - `data/interim/` = intermediate artifacts (cacheable; may be large)
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

All *per-run* stages in the pipeline share the same `run_id`.

### Artifact types: curated vs run outputs

- **Curated artifacts**: stable, reusable inputs (cacheable)
  - live under `data/curated/<paper>/...`
  - not tied to a particular run_id

- **Run outputs**: figures/tables/regressions tied to a run
  - live under `outputs/<paper>/<run_id>/...`

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
- ✅ migrated: uses new repo paths + `paper/run_id` conventions
- 🟡 hybrid: reads some legacy inputs but writes new outputs
- ❌ legacy: still assumes `Code/out/...` style locations

| Stage ID | Name | Status | Entrypoint | Notes |
|---|---|---:|---|---|
| S0 | LMMD scoring from MDNA text | ✅ | `src/sentiment/run_lmmd_score.py` (or equivalent) | Produces `data/curated/<paper>/panel/lmmd_scores_nikkei225.csv` (+ QC JSON) |
| S0.5 | Market model (alphas/betas) | ✅ | `src/event_study/run_market_model.py` | Produces `data/curated/<paper>/event_study/alphas_betas.csv` |
| S0.75 | Build regression panel (GPT + LMMD merge) | ✅ | `src/panel/build_panel.py` (or equivalent) | Produces `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv` |
| S1 | Event Study: CAR compute + baseline regressions | ✅ | `src/event_study/run_event_study_all.py` | Writes to `outputs/<paper>/<run_id>/event_study/` |
| S2 | Event Study: Horse-race regression grid (GPT vs LMMD) | ✅ | `src/event_study/run_event_study_horserace.py` | Reads CARs from S1 outputs; writes summary to same folder |

---

## Stage Contracts

### S0 — LMMD scoring from MDNA text

**Stage ID:** S0  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `lmmd_score = true`)

#### Purpose
- For each filing listed in an index CSV, load the corresponding `*.mdna.txt`
- Tokenize Japanese text (Sudachi SplitMode.C) and score with LMMD (+/- sets)
- Emit a row-level LMMD score table matching the legacy schema

#### Inputs
- `index_csv` (curated): list of filings to score (must include `filename`, `edinet_code`, `symbol`, and a filing date column)
  - typically: `data/curated/<paper>/panel/mdna_summary_nikkei225_filtered.csv`
- `mdna_root` (interim): directory containing `data/<edinet_code>/<stem>.mdna.txt`
  - typically: `data/interim/<paper>/data/`
- `lmmd_dict_csv` (shared): LMMD dictionary CSV with Japanese token column + Positive/Negative flags

#### Key assumptions
- MDNA file naming:
  - if `filename = S100CKNI_1.xbrl` then expected MDNA text is:
    - `.../<edinet_code>/S100CKNI_1.mdna.txt`

#### Outputs
- `data/curated/<paper>/panel/lmmd_scores_nikkei225.csv`
- QC sidecar JSON next to output:
  - `lmmd_scores_nikkei225.csv.qc.json`

#### Notes
- This stage is intentionally designed to match legacy output columns so downstream steps don’t break.

---

### S0.5 — Market model (alphas/betas)

**Stage ID:** S0.5  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `market_model = true`)

#### Purpose
- Estimate (alpha, beta) per ticker against the market index (e.g., TOPIX)
- Save `alphas_betas.csv` for later abnormal return computation

#### Inputs (curated)
- Market index prices:
  - `data/curated/<paper>/prices/TOPIX_prices.csv` (or shared)
- Stock prices (long format):
  - `data/curated/<paper>/prices/prices_long.csv` (or shared)

#### Output (curated)
- `data/curated/<paper>/event_study/alphas_betas.csv`

#### Skip condition
- Considered done if `alphas_betas.csv` exists (configurable via `skip_if_exists`)

---

### S0.75 — Build regression panel (merge GPT + LMMD)

**Stage ID:** S0.75  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `build_panel = true`)

#### Purpose
- Merge GPT panel with LMMD scores into one regression-ready panel
- Preserve the legacy column names/shape as closely as possible (for compatibility)

#### Inputs (curated)
- GPT panel:
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_filtered.csv`
- LMMD scores:
  - `data/curated/<paper>/panel/lmmd_scores_nikkei225.csv`

#### Output (curated)
- `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv`
- Optional QC sidecar JSON (if wired): `...csv.qc.json`

---

### S1 — Event Study: CAR compute + baseline regressions

**Stage ID:** S1  
**Module:** `src/event_study/run_event_study_all.py`  
**Called by:** `scripts/paper1/run_pipeline.py` (stage `event_study = true`)

#### Purpose
For each event window:
- compute CAR per `(Ticker, EventDate)`
- save a per-window CAR dataset
- run a clustered regression and save a regression summary (per sentiment column)

#### Inputs (current)
- Event-level sentiment panel (curated):
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv`
- Market model params (curated):
  - `data/curated/<paper>/event_study/alphas_betas.csv`
- Stock prices (curated):
  - `data/curated/<paper>/prices/prices_long.csv`
- Market index prices (curated):
  - `data/curated/<paper>/prices/TOPIX_prices.csv`

#### Parameters
- `sentiment_col`: e.g., `document_score`, `lmmd_net`, `neg_rate`, `pos_rate`
- `windows`: e.g., `[(0,0), (0,1), (-1,1), (-2,2), (-3,3)]`
- `paper`, `run_id`: used for output routing

#### Outputs (per run)
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
- load CARs from S1 output
- merge CARs with multiple sentiment measures
- run a grid of regression specifications:
  - GPT only
  - LMMD only
  - GPT + LMMD (horse-race)
  - LMMD components (pos/neg) diagnostics
- optional Year FE / Industry FE toggles
- write one combined summary table

#### Inputs (current)
- Panel CSV (curated; may still attach industry using legacy mapping, depending on implementation):
  - `data/curated/<paper>/panel/mdna_summary_nikkei225_with_lmmd.csv`
- CAR files from S1 (expects *document_score* CARs as the dependent variable source):
  - `outputs/<paper>/<run_id>/event_study/car_results_all_{w0}_{w1}_document_score.csv`

#### Outputs (per run)
- `outputs/<paper>/<run_id>/event_study/horserace_summary.csv`

#### Notes
- This stage z-scores sentiment measures in-sample for comparability across measures/specs.
- CAR is treated as sentiment-invariant and used as the dependent variable.

---

## Config Reference (Paper 1)

All stages are controlled by `configs/paper1/pipeline.toml`.

Common patterns:
- `skip_if_exists = true` is used to avoid recomputation for expensive stages
- Stage-specific inputs are typically relative to repo root and resolved inside the driver

---

## Migration Notes / Next Targets

- Industry mapping for horserace is a remaining “hybrid” dependency if it still reads legacy mapping files.
  - Target: move mapping sources under `data/curated/shared/mapping/` and reference via config.

- Future stages likely needed (Paper 1):
  - EDINET extraction (XBRL -> MDNA text) if you want to reproduce `*.mdna.txt` generation in-repo
  - GPT sentiment generation (costly: OpenAI tokens) with resumable job tracking under `data/interim/<paper>/sentiment_jobs/`

---

## Glossary / Notes

- **CAR**: cumulative abnormal return over an event window (e.g., [-1,1])
- **EventDate**: filing date aligned to a trading day
- **LMMD**: lexicon-based tone measures (net, pos_rate, neg_rate)
- **GPT sentiment**: LLM-derived `document_score` (or equivalent)
- **run_id**: ties all artifacts for one pipeline execution together

---

## Changelog

- 2026-03-07: Added pipeline skeleton + initial stage contracts.
- 2026-03-09: Added S0 LMMD scoring, clarified curated vs run outputs, updated stage inventory (S0–S2 all migrated).
