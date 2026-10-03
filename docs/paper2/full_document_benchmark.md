# Paper 2 — Frozen Full-Document Benchmark Memo

**Date:** 2026-10-03  
**Status:** Frozen benchmark checkpoint

## Purpose

This memo records the completed whole-document benchmark before the project
moves to the advisor-driven persistent/revised/new text decomposition.

The benchmark asks whether the association between sentiment and filing-period
market reaction depends on whole-document textual novelty. It is not intended
to identify whether the relevant sentiment is specifically located in newly
introduced text.

## Frozen empirical design

The benchmark uses:

- 33,046 research-eligible adjacent annual-report pairs;
- primary textual novelty: `noveltyCNum`;
- length control: `absLogLengthChange`;
- fiscal-year fixed effects;
- firm-clustered standard errors by `edinetCode`;
- CAR windows `[0,0]`, `[0,1]`, and `[-1,1]`;
- three sentiment measures: LMMD, Japanese Financial BERT, and GPT-6 Sol;
- two sentiment definitions: current level and year-over-year change.

The regression family is therefore:

``` text
3 sentiment models × 2 sentiment definitions × 3 CAR windows = 18 regressions
```

with the common specification:

``` text
CAR
  ~ sentiment
  + noveltyCNum
  + sentiment × noveltyCNum
  + absLogLengthChange
  + fiscal-year fixed effects
```

## Core benchmark results

The strongest interaction is GPT-6 Sol level sentiment × novelty for
CAR `[-1,1]`:

``` text
interaction coefficient = 0.055811
t-statistic             = 3.031
raw p-value              = 0.002434
Bonferroni p-value       = 0.043814
Holm p-value             = 0.043814
```

This is the only interaction that survives 5% family-wise correction when all
18 interaction tests are treated as one family.

Financial BERT level sentiment shows the same qualitative interaction pattern
for `[-1,1]` (`p = 0.005881` raw) but does not survive the 18-test Holm
correction. LMMD's strongest pattern appears in sentiment change rather than
current level.

## Economic magnitude

Stage 8C reports the effect of a one-sample-standard-deviation increase in
sentiment at selected novelty percentiles.

For GPT-6 Sol level sentiment and CAR `[-1,1]`:

| Novelty percentile | Effect | 95% CI |
|---:|---:|---:|
| 25th | +0.8 bp | -5.5 to +7.1 bp |
| 50th | +3.2 bp | -2.6 to +9.0 bp |
| 75th | +7.2 bp | +1.4 to +13.0 bp |
| 90th | +14.3 bp | +6.1 to +22.5 bp |
| 95th | +21.8 bp | +9.6 to +34.0 bp |

For Financial BERT level sentiment and CAR `[-1,1]`, the corresponding effect
rises from approximately +1.5 bp at the 25th novelty percentile to +18.1 bp at
the 95th percentile.

LMMD change has an upward-sloping conditional effect, but its 95% confidence
interval crosses zero at all five reported novelty percentiles.

## Interpretation

The benchmark supports the narrow statement:

> Whole-document contextual/generative sentiment is more strongly associated
> with short-window market reactions when disclosure language is more
> textually novel.

The result is strongest for GPT-6 Sol level sentiment over `[-1,1]`.

The benchmark does **not** establish that sentiment specifically within newly
introduced or revised language drives the return response. Whole-document
novelty and whole-document sentiment remain aggregate measures.

## Why the benchmark is frozen here

The benchmark was specified before the full three-model results were observed,
and the same architecture was applied consistently to LMMD, Financial BERT,
and GPT-6 Sol. Stage 8C adds interpretation and multiple-testing adjustment
without changing the underlying regression specifications.

Further specification changes motivated by the observed significance pattern
would blur the distinction between the completed benchmark and the new primary
research design. Therefore:

- the 18-regression benchmark is frozen;
- raw, Bonferroni, and Holm p-values are preserved;
- marginal-effect tables and confidence-band figures are frozen;
- the pre-LLM first-look checkpoint remains preserved separately.

## Next research phase

The next phase asks:

> Do markets respond differently to sentiment in persistent disclosure
> language versus newly introduced or substantially revised language?

The next implementation should begin with a small manually inspectable sample
of consecutive filings, validate a passage-alignment/classification method,
and only then scale to the full corpus.
