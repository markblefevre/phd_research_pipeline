# Paper 2 — Next Steps

**Updated:** 2026-10-03  
**Strategy:** The full-document sentiment × novelty benchmark is complete and frozen. The next phase is the advisor-driven persistent-versus-new/revised disclosure design.

## 1. Freeze and preserve the completed benchmark

- [x] **Complete GPT-6 Sol scoring.** Full-corpus scoring is complete for **37,473 / 37,473** research-universe documents using the frozen Sol + prompt-v2 specification.
- [x] **Freeze full-document sentiment outputs.** LMMD, Japanese Financial BERT, and GPT-6 Sol document-level outputs are complete and integrated on the same research universe.
- [x] **Complete and freeze Stage 7.** Historical venue eligibility has been audited and Stage 7A–7F are complete.
- [x] **Construct the regression-ready benchmark panel.** Stage 8A contains **33,046 rows × 135 columns** with zero missing current/prior/change sentiment values for all three sentiment families.
- [x] **Run the fixed benchmark regressions.** Stage 8B contains **18 regressions**: 3 sentiment models × 2 sentiment definitions × 3 CAR windows.
- [x] **Add interpretation and multiple-testing controls.** Stage 8C computes standardized marginal effects, 95% confidence intervals, Bonferroni-adjusted p-values, Holm-adjusted p-values, and publication-style confidence-band figures.
- [x] **Preserve the benchmark memo and outputs.** Treat the full-document analysis as a completed benchmark checkpoint rather than as the final identification strategy.
- [ ] **Create the Git freeze checkpoint.** Commit Stage 8A–8C code, pipeline wiring, `pipeline.toml`, `STATUS.md`, `pipeline_overview.md`, `NEXT_STEPS.md`, and the benchmark memo after confirming `git status` contains no unrelated changes.

### Frozen Stage 7 / Stage 8 checkpoint

| Measure | Final count |
|---|---:|
| Research pairs | 33,046 |
| Stage 7A eligible | 32,126 |
| Stage 7D models estimated | 31,698 |
| CAR(0,0) final sample | 31,241 |
| CAR(0,1) final sample | 31,069 |
| CAR(-1,1) final sample | 30,926 |
| Stage 8A empirical-panel rows | 33,046 |
| Stage 8B regressions | 18 |
| Stage 8C marginal-effect rows | 90 |

### Frozen benchmark result

The strongest interaction is GPT-6 Sol **current-level sentiment × textual novelty** for CAR `[-1,1]`:

``` text
interaction coefficient = 0.055811
t-statistic             = 3.031
raw p-value              = 0.002434
Bonferroni p-value       ≈ 0.0438
Holm p-value             ≈ 0.0438
```

At the **95th percentile of textual novelty**, a one-standard-deviation increase in Sol sentiment is associated with approximately **+21.8 bp** of CAR `[-1,1]`, with an approximate 95% confidence interval of **+9.6 to +34.0 bp**.

This result should be retained as the **whole-document benchmark**. It does not identify whether the market response comes specifically from sentiment in changed text.

**Stop condition:** The full-document benchmark is frozen. Do not add alternative LLMs, additional CAR windows, or significance-driven specification changes unless a concrete data-quality problem is discovered.

## 2. Primary pivot: persistent versus new/revised disclosure

**Core question:** Do markets respond differently to sentiment in persistent disclosure language versus newly introduced or substantially revised language?

The first objective is not to score sentiment again. It is to construct a defensible **text decomposition** that identifies where disclosure content is persistent versus changed.

### 2A. Define the decomposition

- [ ] Choose the primary alignment unit: sentence, paragraph, or short passage.
- [ ] Define operational categories:
  - **persistent / repeated**
  - **substantially revised**
  - **new**
- [ ] Preserve `revised` and `new` separately in intermediate data even if the main empirical specification later combines them into a single `changed` component.
- [ ] Define how to handle reordered text so that moved but unchanged language is not falsely classified as novel.
- [ ] Define similarity thresholds before examining market-response results.
- [ ] Decide whether matching should use lexical similarity, embeddings, or a staged hybrid approach.

### 2B. Pilot before scaling

- [ ] Build a small manually reviewed pilot sample of consecutive filings.
- [ ] Include representative firms already used in development, such as Toyota and MUFG, plus several cases with:
  - low document-level novelty;
  - high document-level novelty;
  - large MD&A length changes;
  - obvious disclosure restructuring.
- [ ] For each pilot pair, inspect:
  - passage alignment quality;
  - false novelty from formatting/reordering;
  - false persistence from generic boilerplate;
  - treatment of inserted/deleted sections;
  - surrounding context preservation.
- [ ] Compare candidate decomposition methods on the same manually inspected examples.
- [ ] Freeze the decomposition method before full-corpus execution.

**Decision gate:** Do not scale to all 33,046 pairs until the passage-level decomposition is manually defensible.

## 3. Component-level sentiment

After the decomposition is frozen:

- [ ] Construct text components for each filing pair:
  - persistent text;
  - revised text;
  - new text;
  - optionally combined changed text = revised + new.
- [ ] Compute component size measures:
  - characters;
  - tokens;
  - passage counts;
  - share of current MD&A.
- [ ] Measure sentiment separately within each component.
- [ ] Start with methods that can be computed cleanly from the decomposed text:
  - LMMD;
  - Financial BERT;
  - GPT-6 Sol only if the existing unit-level scores can be mapped reliably, otherwise evaluate whether a targeted rerun is justified.
- [ ] Keep sentiment measurement separate from the text-selection algorithm.
- [ ] Define minimum component-size rules before regression analysis.
- [ ] Preserve short/empty-component flags rather than silently imputing sentiment.

### Important implementation principle

Do **not** assume the full 37,473-document Sol corpus must be rerun.

The first task is to determine whether the existing Sol unit-level JSON can be mapped cleanly to persistent versus changed passages. If not, consider a **targeted component-level Sol rerun** only after the decomposition is frozen. The decomposition itself should not depend on Sol.

## 4. Primary component-level regressions

The central immediate-response specification should compare sentiment located in different disclosure components directly.

A conceptual starting point is:

``` text
CAR_it
  ~ sentiment_persistent_it
  + sentiment_changed_it
  + component-size controls
  + standard firm/report controls
  + fixed effects
```

with a formal test:

``` text
H0: beta_persistent = beta_changed
```

Planned work:

- [ ] Pre-specify the primary component-level sentiment variables and controls.
- [ ] Decide whether `new` and `revised` are combined in the main specification or entered separately.
- [ ] Include component-size controls so sentiment estimates are not mechanically driven by how much text appears in each category.
- [ ] Use matched observations where both persistent and changed components are measurable.
- [ ] Preserve the Stage 7 CAR windows `[0,0]`, `[0,1]`, and `[-1,1]` unless there is a methodological reason to change them.
- [ ] Cluster standard errors consistently with the benchmark.
- [ ] Formally test equality of persistent and changed sentiment coefficients.
- [ ] Compare component-level results with the frozen whole-document benchmark on identical observations where feasible.

**Stop condition:** Do not tune passage thresholds or sentiment construction after observing which version produces stronger return coefficients.

## 5. Secondary extensions

Only after the persistent-versus-changed design is stable:

- [ ] Decide whether forward-looking versus non-forward-looking sentiment adds enough incremental value to warrant a separate analysis.
- [ ] Consider a two-dimensional decomposition only if empirically and conceptually useful:

``` text
                    Persistent        Changed
Forward-looking     PF sentiment      CF sentiment
Other               PO sentiment      CO sentiment
```

- [ ] Design longer-horizon return tests separately. Treat delayed incorporation as a hypothesis, not an assumption.
- [ ] Revisit the previously planned whole-document robustness suite only where it remains relevant:
  - C-raw novelty;
  - FY2018 exclusion;
  - signed length change;
  - top 5% / top 10% absolute-length-change exclusions;
  - industry fixed effects.

These are secondary to establishing the new primary design.

## 6. Deliverables and decision gates

| Gate | Required output | Decision |
|---|---|---|
| A | Full-document benchmark committed and documented | Freeze original design |
| B | Manually validated passage-alignment pilot | Approve decomposition method |
| C | Full-corpus persistent/revised/new decomposition | Freeze text-selection layer |
| D | Component-level LMMD/BERT/Sol sentiment dataset | Freeze sentiment inputs |
| E | Immediate-response component regressions and coefficient-equality tests | Assess central hypothesis |
| F | Selected robustness and longer-horizon tests | Prepare final empirical narrative |

## 7. Document roles

- `STATUS.md`: factual completion state, blockers, current counts, and next execution step.
- `pipeline_overview.md`: implemented stage architecture, inputs/outputs, code paths, and frozen implementation decisions.
- **`NEXT_STEPS.md` (this file):** research priorities, decision gates, stop conditions, and planned work.
- `full_document_benchmark_2026-10-03.md`: frozen interpretation of the completed whole-document benchmark.
- `DAILY_LOG.md`: optional dated research journal; historical entries are not the current work plan.

**Immediate next action:** Finish the Git freeze commit for the full-document benchmark. Then design and manually test the persistent/revised/new passage decomposition on a small set of consecutive filings before writing any full-corpus Stage 9 code.
