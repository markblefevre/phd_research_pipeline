# Paper 2 — Stage 8I statistical identification diagnostics

**Scope:** A diagnostic checkpoint, not additional model selection. No LLM calls.
It consumes the validated Stage 8E scores, the frozen Stage 8A panel/Stage 7
outcomes, and the Stage 8H adjusted-control file. It **does not overwrite**
Stages 6–8H or the repo's `STATUS.md`/`PIPELINE.md`.

## Install

Put `run_stage8i_statistical_diagnostics.py` and
`test_stage8i_statistical_diagnostics.py` in `scripts/paper2/` alongside the
already-committed Stage 8E, Stage 8F and `run_stage8_twfe.py` scripts.

From the repository root with Conda `(paper2)`:

```bash
python -u scripts/paper2/test_stage8i_statistical_diagnostics.py
python -u scripts/paper2/run_stage8i_statistical_diagnostics.py
```

Default Stage 8H controls:

`data/interim/paper2/controls/stage8h_adjusted_market_industry_event_controls.csv`

Default results directory:

`data/interim/paper2/regressions/stage8i_identification_diagnostics/`

If the directory exists, specify another:

```bash
python -u scripts/paper2/run_stage8i_statistical_diagnostics.py \
  --output-dir data/interim/paper2/regressions/stage8i_identification_diagnostics_rerun
```

## Inferential choices and their limitations

* **Retrospectively designated main outcome:** CAR[-1,+1] on the Stage 8F
  common three-window sample, complete-case three-way scores, firm and fiscal
  year FE, Stage 8H controls, firm-clustered CR1 standard errors, t(G-1).
* **Exploratory timing outcome:** Day +1 abnormal return defined exactly as
  CAR[0,+1] − CAR[0,0] on the same sample.
* **Presence-indicator sensitivity:** Empty category scores entered as zero
  alongside presence dummies (absence is NOT neutral tone). Interpretation of
  the tone coefficient remains conditional on category presence.
* **Joint equality:** Two restrictions comparing three jointly included
  coefficients; approximate Wald F(2,G-1) using the same cluster covariance.
  This is a large-cluster approximation, not an exact finite-sample test.
* **Pairwise inference:** New-minus-unchanged, new-minus-revised,
  revised-minus-unchanged, with observed 95% cluster confidence intervals.
  Holm adjustments reported for the 3 main CAR/complete contrasts and a
  stricter family across all 12 three-way exploratory contrasts. Further
  families are explicitly labeled in `multiple_testing_adjusted.csv`.
* **Precision:** 80%/90% minimum detectable coefficient differences at 5%
  two-sided size using a noncentral-t approximation **holding the realized
  standard errors fixed**. These are *not* post-hoc power estimates.
* **Collinearity:** Pairwise raw and firm/year-demeaned correlations,
  conditional VIFs from all FE-residualized covariates, covariance and
  correlation of coefficient estimates, and firm-level score variation.
* **Component-size diagnostics:** Score/length distributions, short-text
  proportions, score extremity by category length and observed score-presence
  patterns. Length is a *proxy* for possible measurement stability; none of
  these describes true scoring error without independent manual labels.
* **Exploratory exposure sensitivity:** Component score multiplied by share
  of characters across only unchanged, revised and new categories, with
  revised and new shares as covariates (unchanged omitted due sum-to-one).
  This measures sentiment exposure, NOT the original tone-intensity effect.
  Category absence yields structural zero exposure, not imputed neutral tone.
  If nonzero characters remain but the scorer produced no score (e.g. no
  narrative units), exposure is missing and that filing is excluded from
  the exposure regression, not treated as neutral.
  It is explicitly **not** a replacement main specification.
* **Frozen binary comparison:** Stage 6F-B persistent/novel sentiment scores
  already in the Stage 8 panel, both over their full eligible sample and
  restricted to the same filing IDs as the three-category samples. The binary
  and three-category models use **different classifications/estimands**;
  comparison is diagnostic rather than a contest over significance.
* All tests are exploratory in the sense that the outcomes were examined in
  earlier work. Nothing here claims prospective preregistration, causal
  effects or proof of first public information arrival.

## Outputs

Review these first:

| File | Meaning |
|---|---|
| `coefficient_contrasts.csv` | Pairwise differences, clustered SE, 95% CI, raw p |
| `joint_equality_tests.csv` | Wald omnibus equal-slopes tests |
| `multiple_testing_adjusted.csv` | Explicit Holm families |
| `sample_diagnostics.csv` | Samples and coverage per measurement design |
| `minimum_detectable_differences.csv` | MDE at 80%, 90% approximate power |
| `sentiment_coefficient_covariance.csv` | Cluster covariance and coefficient correlations |
| `contrast_variance_decomposition.csv` | Why a difference SE is large: two variances and covariance |
| `conditional_vif.csv` | VIF after FE absorption, other controls included |
| `predictor_correlations.csv` | Raw and FE-residual sentiment correlation |
| `within_firm_variation.csv` | Within-firm and FE residual score variation |
| `component_length_summary.csv` | Length and missingness across categories |
| `component_length_bins.csv` | Score distribution by character-count bins |
| `component_presence_patterns.csv` | Availability patterns and mean outcomes |
| `model_summary.csv` / `coefficients.csv` | Full models and all terms |
| `run_metadata.json` | Provenance, SHA-256 inputs, limitations, design |

After you run, provide the final terminal output plus these CSVs (particularly
contrasts, joint equality, multiple testing and sample diagnostics) before
we update `STATUS.md` and `PIPELINE.md` and make the checkpoint commit.
