# Paper 2 — Sample attrition and Stage 8I identification checkpoint

**Checkpoint date:** 2026-10-20  
**Status:** Aggregate funnel reconciled to frozen Stage 3 / Stage 7 records and Stage 8I output; row-level control-missingness and Stage 8D-to-8H migration require running `scripts/paper2/audit_stage8i_sample_attrition.py` on the canonical local files.  
**Primary estimand:** Three jointly included component-tone slopes, complete-case, CAR[-1,+1], firm/year fixed effects, eight Stage 8H controls, firm-clustered CR1.

## Data funnel

| Stage | Retained | Removed from prior | Explanation |
|---|---:|---:|---|
| Stage 1 EDINET Annual Securities Reports | 37,807 | — | Downloaded filing records |
| Stage 2 successful Japanese MD&A extraction | 37,757 | 50 | Extractor failures, documented as foreign-company filings |
| Stage 3 contiguous standard annual reporting-period pairs | 33,046 | 4,711 | 4,441 initial issuer observations without an in-universe predecessor, 268 transition-period pairs, 2 noncontiguous pairs |
| Stage 7A TSE-eligible events | 32,126 | 920 | 814 regional non-TSE; 57 not in TSE price universe; 26 already delisted; 15 insufficient listing history; 8 TOKYO PRO |
| Stage 7E valid CAR[-1,+1] and Stage 8I common event windows | 30,926 | 1,200 | 428 market models not estimated; 772 missing required stock returns |
| Stage 8I event-window + controls/identifiers complete | 29,227 | 1,699 | Complete cases for firm/year IDs and eight regression controls; **not** missing sentiment categories (which are represented by presence flags in this design) |
| Stage 8I complete case for all three sentiment categories | 21,540 | 7,687 | Some unchanged/revised/new components are structurally absent, leading to missing category-level sentiment |

**Accounting identity:** 37,807 - 50 - 4,711 - 920 - 1,200 - 1,699 - 7,687 = **21,540**.  
Relative to 33,046 eligible pairs, the primary sample retains **65.18%** and the presence-indicator sample retains **88.44%**. Relative to 30,926 valid event windows, primary retains **69.65%**.

### Why Stage 3 excludes 4,711 filings

The 37,757 extracted MD&A reports represent 4,441 EDINET codes, so 4,441 reports have no immediately prior *observed* filing from the same code. That leaves 33,316 adjacent-period comparisons; 268 are transition-period comparisons and 2 are noncontiguous. The latter 270 are not admissible standard year-on-year comparisons, leaving 33,046 standard annual pairs. No evidence of broad extraction failure is implied by this reduction.

### Why 7,687 three-component observations are unavailable

Across the **full** 33,046 pair corpus (not the 29,227 control-complete subsample), sentiment availability is:

| Component | Scored / present | Absent / empty |
|---|---:|---:|
| Unchanged | 31,926 | 1,120 |
| Revised | 33,029 | 17 |
| New | 24,991 | 8,055 |
| All three present | 24,105 | 8,941 have at least one component missing |

The counts overlap: a filing may lack several categories. All 89,946 **nonempty** components were scored or resumed; zero scoring failures. Absent components are deliberately recorded as **missing**, not as sentiment zero. Of the 29,227 observations passing event-window and control tests, exactly 21,540 have all three scores. The 7,687 excluded due to absent categories belong to this narrower 29,227-observation denominator and **must not** be confused with the full-corpus 8,941.

The presence-indicator specification retains the 29,227 rows by zero-filling structurally absent score *regressors* **and explicitly including category-presence indicators**. The score conditional on presence is the interpretation; zero is not assumed to mean neutral sentiment.

### Why Stage 8I has fewer rows than Stage 8F

| Treatment | Stage 8F / Stage 8D controls | Stage 8I / Stage 8H adjusted controls | Net N change |
|---|---:|---:|---:|
| Complete-case | 22,166 | 21,540 | -626 |
| Presence indicators | 30,188 | 29,227 | -961 |

Stage 8H replaced earlier raw-price-based market controls with adjusted versions. The two sample sizes alone show a **net** change, *not* the exact count lost on a filing-ID basis; some filings could have entered as others exited. Run the row-level audit below to determine lost/gained document IDs and specific missing control values.

The original Stage 8I `sample_diagnostics.csv` column `dropped_missing_covariates` (9,386 for complete cases) is mislabeled: it equals 30,926 - 21,540 and **combines** 1,699 incomplete-control/identifier rows and 7,687 missing-component rows.

## Stage 8I inference checkpoint (not a post-hoc specification search)

Primary complete-case CAR[-1,+1], N=21,540, firms=4,044:

- New sentiment beta +0.2495, p=0.0311 (marginally significant unadjusted).
- New minus unchanged +0.2471, 95% CI [-0.1318, +0.6260], p=0.2011.
- New minus revised +0.0908, 95% CI [-0.3376, +0.5192], p=0.6777.
- Equal-slopes joint test F(2,4043)=0.8176, p=0.4416.
- Conditional VIFs 1.009–1.166: precision shortfall cannot be attributed to high conditional collinearity.
- Holm adjustment across the primary three contrasts finds no significant difference. When Holm-adjusting all four tested new-slope coefficients together, only the presence-indicator AR[+1] result survives 5% (adjusted p=0.0346). This family is exploratory; do not call it prospective preregistration.

**Interpretation:** New-language sentiment shows a positive association with CAR in reported regressions, but there is insufficient evidence that its market slope is *stronger* than revised- or unchanged-language sentiment. A nonsignificant difference is not proof of equal slopes, and the three-category and older binary decomposition are different estimands.

## Run the row-level audit before final documentation and commit

From the repo root, Conda environment `(paper2)`:

```bash
python scripts/paper2/audit_stage8i_sample_attrition.py
```

The script imports the **existing** frozen Stage 8E and Stage 8F functions. It does not rescore text or refit regressions. It asserts both exact frozen sample sizes **and document-ID equality** with the Stage 8F selection logic. It refuses to overwrite an output directory.

Outputs in `data/interim/paper2/regressions/stage8i_sample_attrition_audit/`:

- `attrition_funnel.csv`: reconciled stage counts.
- `filing_level_attrition.csv`: one row per research pair and sequential exclusion reason (33,046 rows).
- `missing_controls.csv` and `missing_control_patterns.csv`: identify the exact variable(s) excluding the 1,699 event-window-complete rows; missingness overlaps across controls.
- `component_presence_patterns.csv`: 3-bit unchanged/revised/new indicator patterns **within 29,227 control-complete rows**, pinpointing the 7,687 component omissions.
- `attrition_by_fiscal_year.csv`: whether attrition is concentrated in certain years.
- `selection_profile.csv`: firm coverage and sample average CAR by exclusion group.
- `stage8d_to_8h_sample_migration.csv`: previous-to-adjusted control samples with exact lost/gained/retained IDs (if Stage 8D file is present).
- `audit_manifest.json`: source hashes and audit definitions.

After reviewing that output, update top-level `STATUS.md` and `PIPELINE.md` with the verified findings, preserving the existing stage history. Keep large raw/model outputs outside Git if ignored, but preserve their stable paths, run IDs, hashes, and small validation diagnostics. The four Stage 8I CSVs supplied to this checkpoint are under `docs/paper2/stage8i_diagnostics/`.

## Authoritative sources

- Repo `PIPELINE.md`, Stage 1–3 and Stage 7A–F frozen run summaries.
- `scripts/paper2/run_stage8e_validate_regressions.py`: Stage 8E sentiment join, eight controls, event study flags.
- `scripts/paper2/run_stage8f_event_day_decomposition.py`: common event-window, missing-category, and `.dropna` criteria.
- `scripts/paper2/run_stage8i_statistical_diagnostics.py`: adjusted controls and Stage 8F `shared_sample` reuse.
- `README_STAGE8I.md`, `sample_diagnostics.csv`, `coefficient_contrasts.csv`, `joint_equality_tests.csv`, `multiple_testing_adjusted.csv`.
