# Stage 8C — Marginal Effects and Multiple-Testing Diagnostics

This package adds a separate interpretation stage after the frozen Stage 8B
full-document benchmark regressions.

## Files

- `src/analysis/marginal_effects.py`
- `src/pipeline/stages/marginal_effects.py`

## Runner changes

Add:

```python
from src.pipeline.stages.marginal_effects import run_stage_marginal_effects
```

Then, after the Stage 8B block:

```python
# Stage 8C: standardized marginal effects and multiple-testing diagnostics
if bool(stages.get("marginal_effects", False)):
    run_stage_marginal_effects(paper=paper, cfg=cfg, logger=logger)
else:
    logger.info("Stage marginal_effects disabled")
```

## TOML additions

Under `[stages]`:

```toml
marginal_effects = true
```

Add:

```toml
## STAGE 8C MARGINAL EFFECTS / MULTIPLE TESTING
[marginal_effects]
panel_csv = "data/interim/paper2/analysis/empirical_panel.csv"
baseline_summary_csv = "data/interim/paper2/regressions/baseline/baseline_regression_summary.csv"
output_dir = "data/interim/paper2/regressions/marginal_effects"
figure_dir = "outputs/paper2/figures/regressions/marginal_effects"

windows = [[0,0], [0,1], [-1,1]]
novelty_column = "noveltyCNum"
length_control = "absLogLengthChange"
sentiment_models = ["lmmd", "financial_bert", "llm"]

include_year_fixed_effects = true
include_industry_fixed_effects = false
cluster_by_firm = true
firm_cluster_column = "edinetCode"

percentiles = [0.25, 0.50, 0.75, 0.90, 0.95]
ci_level = 0.95

formats = ["png", "pdf"]
dpi = 180

# Pre-declared paper plots. All 18 regression combinations still receive
# diagnostic marginal-effect plots and all 18 enter Holm/Bonferroni correction.
publication_specs = ["lmmd:change", "financial_bert:level", "llm:level"]

# Stage 8C refits the same Stage 8B equations only to recover covariance
# matrices. This tolerance verifies that the interaction coefficients and
# raw p-values reproduce Stage 8B.
verification_tolerance = 1e-10
```

## Outputs

Tables:

```text
data/interim/paper2/regressions/marginal_effects/
    marginal_effects.csv
    interaction_multiple_testing.csv
    marginal_effects_summary.json
```

Figures:

```text
outputs/paper2/figures/regressions/marginal_effects/
    diagnostics/
        # one plot for each of the 18 regressions
    paper/
        lmmd_change_all_windows.*
        financial_bert_level_all_windows.*
        llm_level_all_windows.*
```

## Interpretation

For each of the 18 frozen benchmark regressions, Stage 8C reports the effect of
a one-sample-standard-deviation increase in sentiment at the requested novelty
percentiles. The confidence interval uses the full covariance between the
sentiment main effect and the sentiment × novelty interaction.

The multiple-testing family is all 18 interaction tests. Raw, Bonferroni, and
Holm-adjusted p-values are reported. The stage does not choose specifications
based on significance.
