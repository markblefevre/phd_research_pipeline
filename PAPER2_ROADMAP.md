# Paper 2 — Research and Manuscript Roadmap

**Working document | Updated: 2026-11-02**  
**Project:** EDHEC Executive PhD in Finance — Paper 2  
**Status:** Domestic corpus and NLP scoring frozen; corrected J-Quants market rebuild and Stage 8H–8I analyses documented; manuscript Methods substantially drafted. Momentum excluded from primary controls by design; remaining controls, robustness, final inference, Results, and Introduction remain.

> **Purpose:** This is the *decision and delivery tracker* for finishing Paper 2. `STATUS.md` remains the detailed research log and evidence record; `PIPELINE.md` remains the implementation and artifact map. Do not overwrite or re-label frozen results as new primary findings.

## 1. Research identity and boundary with Paper 1

### Paper 1 — complete

**“Measuring Sentiment in Japanese Financial Filings.”** Compares Japanese finance-specific LMMD and GPT document-level sentiment in Nikkei 225 EDINET MD&A filings (2018–2023; 1,087 filings), assessing associations with filing-window CARs. It asks **how alternative sentiment measurement methods affect economic inference**.

### Paper 2 — proposed core contribution

**Working title:** *Old and New Information in Japanese Corporate Disclosures: Evidence from MD&A Sentiment and Market Reactions*.

**Advisor-driven research question:**

> **Do markets respond differently to sentiment located in persistent disclosure language versus newly introduced or substantively revised MD&A language?**

Paper 2 asks **where potentially return-relevant tone is located in a longitudinal disclosure**, rather than principally **which sentiment model performs best**. Textual novelty, information location, and disclosure persistence are the economic constructs; LMMD/BERT/GPT and semantic classifiers are measurement tools.

**Non-negotiable interpretation:** *Textually new* does not necessarily mean *new to investors*. Associations with abnormal returns are not proof of causal EDINET-filing effects; prior earnings announcements, filing timing, contemporaneous news, and other confounds matter.

### Overlap policy

| Topic | Treatment in Paper 2 |
|---|---|
| EDINET and Japanese MD&A setting | Reuse factual/institutional background in concise, adapted prose. |
| LMMD translation/validation | Summarize briefly; Paper 1 contains the detailed construction. Do not duplicate its translation tables. |
| Tokenization, NLP architecture | Describe actual Paper 2 specifications, which are not identical to Paper 1. |
| Event-study market model | Standard methodology may recur, but document Paper 2's J-Quants adjusted-return and submission-time event alignment. |
| LMMD vs GPT comparison | Supporting benchmark, not Paper 2's main research claim. |
| Paper 1 illustrative JFE / Asahi / Alps examples | Avoid reusing as central Paper 2 evidence; select fresh persistent/novel examples. |
| Literature review | Some shared canonical literature is appropriate; foreground Brown & Tucker, Dyer, Cohen, Li, Muslu and information-location/novelty literature rather than model rankings. |

**Different samples and sentiment implementation:** Paper 1 has 1,087 Nikkei 225 filings; Paper 2 has 33,046 consecutive-report pairs from a much broader issuer universe. Paper 2's GPT-6 Sol unit-level, character-weighted document sentiment and GPT-6 Luna component scores should not be described as the same GPT score used in Paper 1.

## 2. Research checkpoint (updated 2026-11-02)

This section supersedes the **2026-10-19** checkpoint for current planning. Earlier analyses and their exact output provenance remain in `STATUS.md`. The claims below are documented from the research sessions and latest manuscript; the uploaded files alone do **not** independently verify repository commits, hashes, or canonical artifact locations.

| Workstream | Status | Current checkpoint |
|---|---|---|
| EDINET and domestic eligibility | COMPLETE | 37,807 initial reports; 37,757 domestic MD&As extracted; 33,046 eligible consecutive standard annual pairs; 37,473 distinct MD&A documents in pair universe. |
| Whole-document novelty and LMMD/BERT/Sol | FROZEN | Baseline `sudachi_c_num` novelty, raw-number alternative, and whole-document sentiment benchmarks retained. |
| Stage 6E binary classification and Stage 6F sentiment | FROZEN | Separate persistent/novel design; no classification/scoring failures remain. |
| Five-way classification / Stage 8E sentiment | FROZEN | 33,046 pairs, 89,946 scored nonempty unchanged/revised/new components; 8,055 genuinely empty new components. |
| J-Quants source data and Stage 7 | REBUILT / RECONCILED (session record) | 121 monthly archives checked; corrected adjustment factors; exactly one filing's market-model/event outcomes affected, E01300 / S100YNKO. Event samples unchanged: 32,126 initially market-eligible and 30,926 CAR [-1,+1] complete. |
| Stage 8H adjusted market controls | REBUILT (session record) | Uses adjusted returns and strictly pre-filing data; momentum excluded from proposed primary regression and retained for robustness; remaining control-set freeze OPEN. |
| Stage 8F–8I corrected regressions | REBUILT / PROVISIONAL INFERENCE | Primary complete-case firm+year FE, CAR [-1,+1]: N=21,540 / 4,044 firms; presence-indicator N=29,227 / 4,148 firms. |
| Manuscript | SUBSTANTIALLY DRAFTED | Latest 35-page PDF (Nov 2): abstract revised; Related Work, Data and Methods substantially drafted; Introduction empty; Results placeholders; Conclusion provisional. |

### Current three-component evidence (corrected-session checkpoint, not final freeze)

In the complete-case firm+year FE CAR [-1,+1] specification (CAR in percentage points per raw sentiment unit), unchanged coefficient ≈ +0.0029 (p≈.986), revised ≈ +0.1588 (p≈.412), and new ≈ +0.2493 (p≈.0312). Direct tests do **not** establish different slopes: new minus unchanged p≈.202, new minus revised p≈.679; joint equality p≈.4434. Nominal significance of the new coefficient is **not** proof that newly introduced tone is more informative. Effect sizes, multiple-testing sensitivity, confidence intervals, and the control-set definition remain to be frozen.

**Preserved interpretation:** text new to the MD&A is not necessarily first public information; CAR associations are not causal filing effects. Retain the 2018 regulatory transition as a potential confound.

**Momentum design decision (2026-11-02):** Exclude historical momentum from the proposed primary regression rather than impose the strict Stage 8H continuous-return history requirement. Retain the existing 20-day and 60-to-5-day momentum variables for robustness, without modifying their construction. The required A/B/C comparison holds the momentum-eligible sample fixed when adding momentum, and separately measures the loss from restricting the sample. Final baseline coefficient estimates and sample sizes **must be rerun** without momentum; the coefficients above are prior corrected-checkpoint results, not the new no-momentum baseline. This is a methodological choice, not an inference of coefficient stability.

## 3. Decision gates — resolve before new modeling

- [x] **D1 — Primary taxonomy (manuscript choice):** five-way-derived `unchanged`/`revised`/`new` is primary; independent Stage 6E persistent/novel is secondary. Preserve separate provenance and labels.
- [x] **D2 — Primary market window (manuscript choice):** `[-1,+1]` is primary, `[0,+1]` and `[0,0]` are narrower comparisons; day −1 may contain earlier announcements.
- [x] **D3 — Primary coefficient contrast (manuscript choice):** `β_new − β_unchanged = 0`, two-sided. Report both other pairwise contrasts and a joint three-way equality Wald test; an individually significant slope does not imply unequal slopes.
- [ ] **D4 — Component measurement:** common raw sentiment scoring scale `[-1,1]`, character-weighted narrative-unit aggregation and missing-for-empty established; finalize minimum-size sensitivity and estimation-sample SD effect sizes.
- [ ] **D5 — Controls and fixed effects:** firm+year FE and firm-clustered SE specified; momentum exclusion from primary model decided, with 20-day and 60-to-5-day pre-filing returns retained as robustness controls. Freeze the other pre-filing covariates and verify common-sample sensitivity before closing D5.
- [ ] **D6 — Multiple-testing family:** name a limited primary hypothesis set and correction/reporting policy; label everything else as exploratory/robustness.
- [x] **D7 — Frozen-data rule:** retain frozen Stage 6E/6F, Stage 8A–8C, and Stage 8E sentiment; no post hoc prompt tuning or selective rescore to improve significance.

**Remaining gate deliverable:** short dated research-design memo freezing D4–D6, exact estimation sample, effect units, inference/multiplicity, and planned tables. D1–D3 and D7 are manuscript-level decisions, not evidence that final inferential results are frozen.

## 4. Remaining research work — prioritized

### P0: Needed for credible central claim

- [x] Run and reconcile corrected three-component main specification and direct contrasts (Stage 8I); [ ] confirm whether independent Stage 6F binary persistent/novel regressions and equality test exist or must be run.
- [ ] Report all component coefficients **with direct Wald tests / confidence intervals for differences**, not just standalone p-values; compare in economically interpretable units (e.g. per SD, with scale defined).
- [ ] Consolidate Stages 8E–8G in **one same-sample results inventory**, distinguishing baseline, hypothesis tests, robustness, and explorations.
- [ ] Analyze **component absence and sample selection**: complete case vs presence indicators vs other justifiable estimands; give N/firms at each filter; explain 8,055 empty `new` components.
- [x] Rebuild Stage 8H pre-filing market controls using adjusted returns; [x] decide to omit momentum from primary regression; [ ] verify three-model same-sample momentum robustness and freeze remaining controls/attrition.
- [ ] Investigate **prior earnings announcements / other public disclosures** and filing-day timing; establish what can be said about *public-information novelty* versus year-over-year *textual novelty*.
- [ ] Apply declared multiple-testing sensitivity and discuss power/precision of primary contrasts. An insignificant difference is not evidence of equality.

### P1: Focused robustness and presentation

- [ ] FY2018 regulatory-transition exclusion; assess whether novelty/LMMD breaks drive results.
- [ ] Raw vs number-normalized whole-document novelty; length-change controls and tails.
- [ ] Alternate primary CAR windows, clustered standard errors and reasonable FE variants with common samples.
- [ ] Classification validity: present 150-sentence benchmark and limitations; review illustrative `unchanged`/`revised`/`new` cases directly in Japanese with English glosses.
- [ ] Prepare exact sample-flow diagram/table from 37,807 filings → 33,046 pairs → venue eligibility → CAR samples → regression complete cases.

### P2: Only if necessary

- [ ] Other novelty definitions, cross-firm common-language measures, new models, extended horizon studies. **Do not initiate by default:** they create scope creep and are not needed to finish the present paper.

## 5. Manuscript checklist (latest Paper 2 PDF: 35 pages, 2026-11-02)

| Section | Current state | Required action |
|---|---|---|
| Abstract | Revised to unchanged/revised/new framing | Refresh only after final inference; retain cautious language. |
| 1 Introduction | Empty | Draft motivation, design, findings, contribution and limits. |
| 2 Related Work | Five coherent subsections, substantially drafted | Check references, copyediting and overlap with Paper 1. |
| 3 Data | Substantial: domestic criterion, attrition and components documented | Add sample-flow and descriptive tables; reconcile primary estimation sample. |
| 4 Methodology | Substantial: adjusted return equation, 3-component models, equality tests, alternatives and robustness | Freeze controls, hypothesis family, effect-size reporting and specific implementation claims. |
| 5 Empirical Results | Subsection headings and opening paragraph, no tables | Produce sample-consistent descriptive, main coefficient/contrast, day/timing and robustness tables, then draft prose. |
| 6 Conclusion | Preliminary placeholder | Finish only after result freeze. |
| References | Substantial | Metadata and capitalization checks. |

**Minor manuscript QC:** remove stray `“‘latex` near end of Section 4.2; replace unresolved `Section X` cross-reference in Results; correct “The controlled specifications includes” to “include”. Avoid treating discussed but unexecuted robustness as completed.

### Recommended main-text display set

1. **Table 1:** sample construction/attrition, filing timestamps and category presence.
2. **Table 2:** descriptive statistics for sentiment components, text shares/lengths, CAR and controls.
3. **Table 3:** primary *joint* component regressions and formal slope contrasts (same sample).
4. **Table 4:** focused robustness including component absence/sample selection and alternative event windows.
5. **Figure 1:** pipeline/taxonomy visual or example of persistent/revised/new Japanese MD&A sentences.
6. **Appendix:** benchmark model-comparison interactions, methodological benchmark validation, supplementary FE grids and timing diagnostics.

Adjust the display set after D1–D6 are resolved; do not invent unestimated results.

## 6. Completion sequence (priority, not promised dates)

1. Freeze D4–D6, exact adjusted-return controls and main regression sample; record provenance, commit and hashes when accessible.
2. Build publication-ready sample flow, descriptive moments/correlations, raw and SD-scale effects, joint regression coefficients and direct contrasts with CIs.
3. Complete only focused missing robustness: binary taxonomy, FY2018 exclusion, component-presence selection, common-sample window and controls sensitivity (including momentum); document multiplicity.
4. Write Section 5; draft Introduction; align abstract and Conclusion with the verified results.
5. Conduct full methodology/results/code consistency and reference audit before advisor submission.

## 7. Next work session — starting order

- [ ] Confirm canonical repository head, corrected Stage 7/8H/8I artifacts, output hashes and version-controlled documentation.
- [ ] Freeze remaining primary controls without momentum; compare no-momentum full sample, no-momentum restricted sample, and momentum-augmented restricted sample; document attrition and units.
- [ ] Record one-page inference decision memo with D4–D6, primary `β_N−β_U`, joint Wald, and multiplicity policy.
- [ ] Generate regression-sample descriptive table and main firm+year FE table with coefficients, firm-clustered SEs, CIs, pairwise/joint tests, N/firms, and per-SD effect sizes.
- [ ] Update this roadmap and `STATUS.md` after verification. Update `PIPELINE.md` only for changed stage contracts, canonical paths or implementation architecture.

## 8. Decision log

| Date | Decision | Evidence / rationale | Artifact / commit |
|---|---|---|---|
| 2026-10-26 | Establish `PAPER2_ROADMAP.md` as completion driver; preserve `STATUS.md` and `PIPELINE.md` as technical sources. | Paper 2 draft is behind implemented pipeline; conceptual boundary with Paper 1 requires sharpening. | This document |
| 2026-11-02 | Primary taxonomy (D1) | Three-component primary; independent binary secondary, documented in manuscript | Paper 2 §4.6 |
| 2026-11-02 | Primary event window (D2) | CAR [-1,+1] primary, narrower windows and day decomposition as sensitivity | Paper 2 §§4.5–4.7 |
| 2026-11-02 | Primary difference test (D3) | β_new − β_unchanged; secondary pairwise and joint equality test | Paper 2 §4.6 |
| 2026-11-02 | Momentum treatment (part of D5) | Exclude momentum from primary three-component regressions; retain 20-day and 60-to-5-day prior-return measures for robustness. Brown and Tucker (2011) do not include momentum as a separate short-window return regressor; Feldman et al. (2010) adjust returns using matched size/book-to-market/momentum portfolios instead. Strict Stage 8H momentum history requirements materially reduce coverage. A/B/C common-sample sensitivity still pending; decision not chosen based on p-values. | Brown and Tucker (2011) §6/Table 5; Feldman et al. (2010) Table 5; Stage 8H checkpoint |
| TBD | Manuscript results freeze | **OPEN** | TBD |

## 9. Evidence sources and update convention

- Paper 1 manuscript: `EDHEC_LEFEVRE_PhD_Paper1_MDandA_Sentiment-3.pdf` (provided 2026-10-26).
- Paper 2 draft: 35-page November 2, 2026 version (`EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (2).pdf` in conversation).
- Research history: `STATUS.md` (2026-11-02 checkpoint prepended; historical Oct 19 material retained).
- Pipeline and artifact map: `PIPELINE.md` (not modified by this documentation-only checkpoint).

**Update rule:** distinguish **COMPLETE/FROZEN**, **EXPLORATORY**, **OPEN**, and **VERIFIED IN REPO**. Do not turn an observed p-value into a conclusion about unequal coefficients without a formal contrast, or characterize year-over-year textual novelty as necessarily new public information.
