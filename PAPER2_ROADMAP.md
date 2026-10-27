# Paper 2 — Research and Manuscript Roadmap

**Working document | Updated: 2026-10-26**  
**Project:** EDHEC Executive PhD in Finance — Paper 2  
**Status:** Major data/NLP pipeline complete; exploratory empirical inference complete through Stages 8E–8G; primary specification, robustness, and manuscript integration remain.

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

## 2. Verified research checkpoint (as documented through 2026-10-19)

| Workstream | Status | Grounded checkpoint |
|---|---|---|
| EDINET corpus / MD&A extraction | COMPLETE | 37,807 filings; 37,757 extracted MD&As. |
| Longitudinal pairs | COMPLETE | 33,046 research-eligible standard annual pairs; 37,473 distinct MD&A documents in pair universe. |
| Whole-document novelty | FROZEN | TF-IDF cosine novelty, primary `sudachi_c_num`; raw-token variant retained. |
| Whole-document sentiment | FROZEN | LMMD, Japanese Financial BERT, GPT-6 Sol; 37,473 documents each. |
| Stages 8A–8C whole-document benchmark | FROZEN | 18 interaction regressions; GPT-6 Sol sentiment × novelty on CAR [-1,+1] raw p≈0.00243, Bonferroni/Holm p≈0.0438. Benchmark only. |
| Stage 6E binary persistent/novel labels | FROZEN | 33,046 pairs / 3,463,989 sentences; zero unresolved classification failures. |
| Stage 6F binary-component sentiment | COMPLETE | 33,046 persistent, 33,037 novel scored; 9 genuinely empty novel components. |
| Separate five-way disclosure-change taxonomy | COMPLETE | `unchanged`, `revised`, `new_candidate`, `non_substantive`, `uncertain`; 33,046 pairs. **Distinct from Stage 6E.** |
| Stage 8E three-component sentiment | COMPLETE | 89,946 nonempty `unchanged`/`revised`/`new` components scored, zero failures. |
| Market reaction / event eligibility | COMPLETE | 32,126 eligible events; CAR [0,0] 31,241, [0,+1] 31,069, [-1,+1] 30,926. |
| Stage 8D pre-filing controls | IMPLEMENTED, PROVISIONAL | Controls available for 32,126 events; raw-close market-control robustness still required. |
| Stage 8E exploratory regressions | COMPLETE, NOT FINAL INFERENCE | Complete-case firm+year FE CAR [-1,+1] N=22,166, 4,099 firms. Unchanged −0.0146 (p=.926); revised +0.1545 (p=.400); new +0.2506 (p=.0251), in CAR percentage points per sentiment unit. |
| Direct three-component contrasts | INCONCLUSIVE | New minus unchanged p=.1583, new minus revised p=.6485; no reported pairwise test significant at 5%. |
| Stage 8F day-by-day regressions | COMPLETE, EXPLORATORY | New-tone day +1 slope +0.1489 (p=.0192), day 0 −0.0201 (p=.760), day −1 +0.1219 (p=.0777) on complete-case common sample; day +1 versus day −1 difference not significant. |
| Stage 8G filing-time interactions | COMPLETE, EXPLORATORY | At/before-close vs after-close interactions not significant. |

**Important:** These are *documented* results, not independently rerun or post-October-19 repository verification. Confirm file hashes, canonical NAS copies, Git commits, and later runs before changing any status label.

### What is established vs not established

**Supported:** Whole-document contextual/generative sentiment tends to be more associated with CAR when documents are textually novel (strongest corrected benchmark evidence for GPT-6 Sol). In three-component exploratory models, `new` sentiment shows a positive association with certain CAR/day +1 outcomes.

**Not established:** New sentiment is statistically *more* return-relevant than persistent/revised sentiment; effects are causally attributable to the EDINET filing; firms' newly written information was not previously available; filing-time groups show significantly different slopes.

## 3. Decision gates — resolve before new modeling

- [ ] **D1 — Primary taxonomy:** choose either (A) frozen Stage 6E `persistent` versus `novel`, or (B) distinct five-way-derived `unchanged`/`revised`/`new`, as the *primary* design. Specify why. Keep the other as a transparent secondary validation/robustness design; do not silently mix labels.
- [ ] **D2 — Primary market window:** select an event window on economic grounds, accounting for the timestamp-aligned event date and risk of pre-filing releases. Document `[−1,+1]`, `[0,+1]`, and `[0,0]` roles before reviewing more estimates.
- [ ] **D3 — Primary coefficient contrast:** designate the equality/difference test that answers “old vs new” (e.g., `β_novel − β_persistent = 0`, or `β_new − β_unchanged = 0`). **Do not infer difference from one coefficient being significant and another not.**
- [ ] **D4 — Component measurement:** declare sentiment normalization, component-length treatment, minimum-size sensitivity, and approach to legitimately empty categories. Missing is not automatically neutral.
- [ ] **D5 — Controls and fixed effects:** freeze a defensible primary control set (pre-filing only), firm/year or year/industry FE, cluster at firm, and a short prespecified sensitivity set.
- [ ] **D6 — Multiple-testing family:** name a limited primary hypothesis set and correction/reporting policy; label everything else as exploratory/robustness.
- [ ] **D7 — Frozen-data rule:** retain Stage 6E/6F and Stage 8A–8C checkpoints; no post hoc prompt tuning or selective rescore to improve significance.

**Deliverable for this gate:** a 1–2 page *Research Design Decision Memo*, dated and version-controlled, including exact estimands, sample, windows, FE, controls, and planned reporting tables.

## 4. Remaining research work — prioritized

### P0: Needed for credible central claim

- [ ] Run/document the main component-level specification for the **chosen primary taxonomy**, if not yet complete. In particular, determine whether Stage 6F binary persistent/novel regression and direct equality test are available; `STATUS.md` documents scores but not a definitive frozen binary-component primary regression.
- [ ] Report all component coefficients **with direct Wald tests / confidence intervals for differences**, not just standalone p-values; compare in economically interpretable units (e.g. per SD, with scale defined).
- [ ] Consolidate Stages 8E–8G in **one same-sample results inventory**, distinguishing baseline, hypothesis tests, robustness, and explorations.
- [ ] Analyze **component absence and sample selection**: complete case vs presence indicators vs other justifiable estimands; give N/firms at each filter; explain 8,055 empty `new` components.
- [ ] Audit adjusted-return consistency of pre-filing market controls (Stage 8D), and rerun focused specifications if needed.
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

## 5. Manuscript checklist (current Paper 2 PDF: 23 pages)

| Section | Current state | Required action |
|---|---|---|
| Title/abstract | Working title `PAPER 2`; abstract still emphasizes model comparison | Rewrite around persistent/novel information-location question; report only warranted results. |
| 1 Introduction | Empty | Write economic motivation, efficient-market intuition *with qualifications*, question, contribution, sample, findings, limitations, roadmap. |
| 2 Related Work | Substantially written (seven subsections) | **Reorder/reframe:** information location, persistent disclosure, change, boilerplate as center; sentiment model literature as methods support. Minimize redundant Paper 1 language. |
| 2.7 Research gap | Still asks whether contextual NLP is more useful on novel documents | Rewrite to ask if *sentiment in changed portions* differs from *persistent portions*. Whole-document interactions are a supporting benchmark. |
| 3.1–3.4 Data | Largely written and grounded | Preserve; update market retention and timing semantics; remove implementation overdetail where it distracts. |
| 3.5 Descriptive Statistics | Placeholder/future tense | Add sample construction, category sizes, sentiment and CAR distributions, firm/industry/year breakdown. |
| 4.1 Sentiment Measurement | Placeholder | Give LMMD/BERT/Sol benchmark **briefly** and Luna component scoring and aggregation **fully**. |
| 4.2 Textual Novelty | Placeholder | Separate document-level TF-IDF cosine novelty from sentence-level semantic classification; define `persistent`, `novel`, five-way categories, and numeric-change rules. |
| 4.3 Event Study | Placeholder | Define TOPIX market model, [−120,−20], 60-obs minimum, J-Quants return adjustment, actual 15:00/15:30 JST market-close alignment. |
| 4.4 Empirical Specifications | Well-developed *whole-document model* equations | Keep as benchmark; **add component-tone joint equations, formal coefficient contrasts, FE/controls, missingness policy**. Do not present model ranking as main question. |
| 4.5 Robustness | Placeholder bullets | Replace with actual implemented robustness and explicitly state unimplemented tests. |
| 5 Empirical Results | Empty | Whole-document benchmark (secondary); primary component results; formal contrasts; timing and robustness; limitations. |
| 6 Conclusion | Empty | Directly answer research question at warranted confidence; distinct contribution from Paper 1. |
| References | Substantial | Cross-check metadata, non-English citations, and empirical claims; ensure consistency with added sections. |
| Figures and Tables | Empty | Add core sample flow, descriptive table, main regression/contrast table, timing robustness table, optional marginal-effect plot and Japanese examples. |

**Draft artifacts to remove:** stray `“‘latex` near the end of Section 2.7; unrendered placeholder bullets in Sections 4.1–4.5; inaccurate future-tense claims that data/regressions are yet to be constructed.

### Recommended main-text display set

1. **Table 1:** sample construction/attrition, filing timestamps and category presence.
2. **Table 2:** descriptive statistics for sentiment components, text shares/lengths, CAR and controls.
3. **Table 3:** primary *joint* component regressions and formal slope contrasts (same sample).
4. **Table 4:** focused robustness including component absence/sample selection and alternative event windows.
5. **Figure 1:** pipeline/taxonomy visual or example of persistent/revised/new Japanese MD&A sentences.
6. **Appendix:** benchmark model-comparison interactions, methodological benchmark validation, supplementary FE grids and timing diagnostics.

Adjust the display set after D1–D6 are resolved; do not invent unestimated results.

## 6. Eight-week target schedule

| Period | Priority | Completion definition |
|---|---|---|
| Oct 27–Nov 2 | Research design freeze + new title/Introduction outline | D1–D7 documented; primary equation/contrast fixed; roles of two taxonomies explicit. |
| Nov 3–9 | Primary component inference | Main regressions, common samples, direct difference tests, effect scaling frozen. |
| Nov 10–16 | Sample selection / controls / multiple testing | Controls audited; missingness handled; multiplicity/power documented. |
| Nov 17–23 | Earnings/news timing assessment | Disclosure availability caveat supported by specific checks, or identified clearly as unresolved limitation. |
| Nov 24–30 | Data and methods manuscript | Sections 3–4 complete; QA counts reconciled. |
| Dec 1–7 | Results manuscript | Section 5, publication-oriented primary tables and confidence intervals complete. |
| Dec 8–14 | Narrative integration | Abstract, Introduction, Related Work and Conclusion all aligned with advisor question. |
| Dec 15–21 | Consistency audit and advisor draft | Complete cross-reference, sample, coefficient, citation, table-note and Paper 1 overlap audit. |

This is a planning target, **not** a claim that unverified tasks will complete on schedule. Empirical results determine how the final paper is framed.

## 7. Next work session — exact starting order

- [ ] Confirm the latest repo state and canonical artifact integrity; record relevant Git commit and output hashes.
- [ ] Decide D1 primary taxonomy (binary Stage 6E vs five-way-derived three-component model).
- [ ] Decide D2 event window and D3 formal primary contrast.
- [ ] Create/freeze the dated Research Design Decision Memo.
- [ ] Run only the missing primary tests and build a one-page **results audit** (estimates, SE, CI, direct contrasts, N, firms, controls, FE, event windows, multiple-test family).
- [ ] Update this file and `STATUS.md` after each decision or new verified result; update `PIPELINE.md` only if implementation/artifact locations change.

## 8. Decision log

| Date | Decision | Evidence / rationale | Artifact / commit |
|---|---|---|---|
| 2026-10-26 | Establish `PAPER2_ROADMAP.md` as completion driver; preserve `STATUS.md` and `PIPELINE.md` as technical sources. | Paper 2 draft is behind implemented pipeline; conceptual boundary with Paper 1 requires sharpening. | This document |
| TBD | Primary taxonomy (D1) | **OPEN** | TBD |
| TBD | Primary event window (D2) | **OPEN** | TBD |
| TBD | Primary difference test (D3) | **OPEN** | TBD |
| TBD | Manuscript results freeze | **OPEN** | TBD |

## 9. Evidence sources and update convention

- Paper 1 manuscript: `EDHEC_LEFEVRE_PhD_Paper1_MDandA_Sentiment-3.pdf` (provided 2026-10-26).
- Paper 2 draft: `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty-2.pdf` (provided 2026-10-26).
- Research history: `STATUS.md` (last updated **2026-10-19**).
- Pipeline and artifact map: `PIPELINE(1).md` (October 2026 update).

**Update rule:** distinguish **COMPLETE/FROZEN**, **EXPLORATORY**, **OPEN**, and **VERIFIED IN REPO**. Do not turn an observed p-value into a conclusion about unequal coefficients without a formal contrast, or characterize year-over-year textual novelty as necessarily new public information.
