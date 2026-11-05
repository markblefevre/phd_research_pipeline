# PhD Research Pipeline

Python research pipeline for two EDHEC Executive PhD in Finance papers analyzing sentiment and market reactions in Japanese corporate disclosures.

- **Paper 1:** Japanese financial sentiment measurement using LMMD and GPT.
- **Paper 2:** Longitudinal MD&A analysis distinguishing unchanged, revised, and newly introduced disclosure language.

## Repository structure

- `src/` — Pipeline and analysis modules
- `scripts/paper1/`, `scripts/paper2/` — Execution and reporting scripts
- `configs/` — Pipeline configuration
- `data/` — Research data and intermediate outputs
- `outputs/` — Generated tables and figures
- `tests/` — Automated tests

## Paper 2: Stage 9

Generate empirical tables and robustness results:

```bash
python scripts/paper2/build_stage9_tables.py
```

Run Stage 9 tests:

```bash
python -m pytest tests/test_stage9.py -q
```

See `PAPER2_ROADMAP.md`, `STATUS.md`, and `PIPELINE.md` for research status and implementation details.

## Environment

Dependencies currently use the local Python research environment. A formal `pyproject.toml` is planned.
