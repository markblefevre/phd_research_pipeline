# Paper 2 — Next Steps

**Updated:** 2026-10-01  
**Strategy:** Finish and freeze the existing full-document sentiment × novelty experiment as a benchmark, then pivot to the advisor's proposed persistent-versus-novel disclosure design.

## 1. Immediate priorities: finish the benchmark

- [ ] **Finish the current Sol run.** It is approximately 95% complete as of this update. Preserve the exact model identifier, prompt version, parameters, run metadata, document IDs, failures, retries, and token/cost records. Check coverage and duplicates; rerun only failed or missing documents if necessary.
- [ ] **Freeze full-document sentiment outputs.** Confirm unique document-level LMMD, Japanese Financial BERT, and Sol scores and consistent scoring direction. Document missingness and any cross-model differences in sample coverage.
- [ ] **Finish Stage 7 historical-venue audit.** After the EDINET archive is available locally, audit **all 32,134 provisionally eligible events**, not only market-model failures. In particular, resolve Tokyo PRO Market and historical listing status at each filing date. Document ambiguous cases rather than assuming eligibility.
- [ ] **Correct and rerun Stage 7.** Revise 7A eligibility, update hardcoded expected counts only after verifying the audit, and rerun `7A → 7B → 7D → 7E → 7F`. Stage 7C TOPIX data need not be rebuilt unless its source changes. Save the final attrition ledger and QC summaries.
- [ ] **Construct the regression-ready benchmark panel.** Start from Stage 7F's full research-pair table; join each sentiment model's *current-filing* score by a validated unique document key. Preserve eligibility, model-estimation, and window-specific CAR flags. Audit join cardinality, missingness, dates, and common-sample counts.

### Current provisional Stage 7 checkpoint

These counts **will change if the venue audit changes eligibility**:

| Measure | Provisional count |
|---|---:|
| Research pairs | 33,046 |
| Stage 7A eligible | 32,134 |
| Stage 7D models estimated | 31,698 |
| CAR(0,0) final sample | 31,241 |
| CAR(0,1) final sample | 31,069 |
| CAR(-1,1) final sample | 30,926 |

## 2. Benchmark regressions: bounded scope

- [ ] Pre-specify a compact set of regressions for each CAR window: sentiment alone; sentiment plus document-level cosine novelty; and sentiment × novelty, with consistent controls and fixed-effects choices.
- [ ] Include the established `absLogLengthChange` control; document any other controls and standard-error clustering choices before examining the results.
- [ ] Run LMMD, BERT, and Sol on their available samples **and** on an identical common sample for defensible cross-model comparisons.
- [ ] Report coefficient estimates, uncertainty, observations, fit statistics, sample attrition, and basic diagnostics. Check the impact of extreme CAR observations without choosing specifications solely for significance.
- [ ] Preserve scripts, configuration, tables, plots, and a short methodology/results memo. Explicitly label these as **full-document benchmark results**, not evidence isolating sentiment in changed passages.

**Stop condition:** Once the benchmark regressions, essential diagnostics, and reproducible outputs are complete, freeze this design. Do not expand to additional LLMs or open-ended tuning unless a concrete data-quality problem requires it.

## 3. Pivot: advisor's proposed central research question

**Core question:** Do markets respond to sentiment in persistent disclosure language or in newly introduced and substantially revised language?

- [ ] Rewrite the proposal and introduction around disclosure persistence, arrival of new information, immediate price incorporation, and possible delayed response. Distinguish Paper 2's economic question from Paper 1's sentiment-method comparison.
- [ ] Design an auditable **passage-level decomposition** of successive Japanese MD&A filings: persistent, substantially revised, and genuinely new passages. Retain revision/new labels separately even if combined into one novel component in the main analysis.
- [ ] Pilot on a small, manually reviewed sample of consecutive filings. Assess passage alignment, false novelty from formatting or reordering, document coverage, and preservation of surrounding context.
- [ ] Freeze the decomposition method and scale it across eligible filing pairs. Retain the existing document-level cosine novelty as a benchmark or robustness measure, not as a substitute for passage classification.
- [ ] Measure LMMD, BERT, and selected LLM sentiment separately for persistent and novel components. Specify how short passages, mixed sentiment, component size, and context are handled.
- [ ] Estimate immediate market-response models comparing component-level sentiment. Formally test differences between persistent and novel coefficients on matched samples and comparable controls.
- [ ] Design longer-horizon tests separately, including expected-return benchmarks, overlapping observations, and inference. Treat delayed incorporation as a hypothesis, not a presumption.
- [ ] Compare the revised design with the frozen full-document benchmark on identical observations wherever feasible. Test whether decomposition and more sophisticated sentiment models add incremental economic information.

## 4. Deliverables and decision gates

| Gate | Required output | Decision |
|---|---|---|
| A | Completed and QC-checked Sol scoring | Freeze full-document sentiment inputs |
| B | Historical-venue audit and rerun Stage 7 QC | Freeze event-study outcomes and sample |
| C | Regression-ready panel, tables, diagnostics, benchmark memo | Freeze original design and pivot |
| D | Validated pilot passage decomposition | Approve full-corpus decomposition |
| E | Component-level sentiment and immediate-response regressions | Assess central research hypotheses |
| F | Longer-horizon tests and matched benchmark comparisons | Prepare revised paper's results narrative |

## 5. Document roles

- `STATUS.md`: factual completion state, blockers, current counts, and next execution step.
- `pipeline_overview.md` (or the project's canonical pipeline summary): stage architecture, inputs/outputs, commands, and implementation decisions.
- **`NEXT_STEPS.md` (this file):** priorities, research decisions, stop conditions, and milestones.
- `DAILY_LOG.md`: optional dated research journal; preserve historical entries, but do not treat old “tomorrow” items as the current work plan.

**Immediate next action:** Complete and QC the nearly finished Sol run. In parallel, finish the venue audit when the local EDINET archive is ready. Then construct the common-sample benchmark regression panel.
