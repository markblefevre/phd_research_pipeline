# Paper 2 — Research and Manuscript Roadmap

**Working document | Updated: 2026-11-05**  
**Project:** EDHEC Executive PhD in Finance — Paper 2  
**Status:** Frozen EDINET/NLP corpus; corrected J-Quants event-study inputs; Stage 9 no-momentum primary regression and direct contrasts established; November 5 manuscript has Sections 2–5.4 drafted with four main-text tables. Filing-time reconciliation, targeted robustness, five-category validation, Introduction, and Conclusion remain.

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

## 2. Current research checkpoint — 2026-11-05

This **supersedes the 2026-11-02 checkpoint for planning**. The earlier numbers and their methods remain in `STATUS.md` as dated research history. The uploaded Stage 9 CSVs, provenance, reporting outputs and November 5 manuscript were inspected in this review; **the Mac/NAS repository's current Git HEAD and full end-to-end pipeline execution were not verified here**. Do not equate a verified uploaded artifact with a verified repository commit.

| Workstream | Current classification | Evidence / qualification |
|---|---|---|
| Stage 1–3 EDINET, extraction, eligibility | COMPLETE / FROZEN | 37,807 manifest reports; 37,757 domestic MD&A extractions; 33,046 eligible consecutive annual pairs from 37,473 unique MD&A documents. |
| Stage 4–6D text representations and whole-document sentiment | COMPLETE / FROZEN | `sudachi_c_num` primary novelty; raw-number alternative; LMMD, Japanese Financial BERT and GPT-6 Sol whole-document sentiment. Historical Stage 8A–8C benchmark outputs preserved. |
| Independent Stage 6E/6F binary persistent/novel design | COMPLETE / FROZEN AS A SEPARATE MEASUREMENT | Do not mechanically combine with five-category primary design; final benchmark inference still needs manuscript reconciliation. |
| Five-category classification and Stage 8E component sentiment | COMPLETE / FROZEN OUTPUTS; VALIDITY WORK OPEN | 33,046 pairs; 3,463,989 current-year sentences; 89,946 nonempty category components scored; 8,055 pairs contain no new category. Targeted **five-category** external/manual accuracy validation is still required; the old binary benchmark does not validate this taxonomy. |
| Corrected J-Quants and Stage 7 event study | REBUILT / DOCUMENTED | 32,126 market/listing-eligible; 31,698 fitted market models; 30,926 complete CAR `[-1,+1]`. Previously audited factor corrections affected one market model/CAR event, without changing event sample counts. |
| Stage 8H pre-filing controls | REBUILT / DOCUMENTED | Corporate-action-adjusted return inputs; strictly pre-filing control windows. Historical momentum omitted from primary by design; 20-day and 60-to-5-day measures reserved for sensitivity. |
| **Stage 9 primary three-component model** | **PRIMARY CHECKPOINT / VERIFIED UPLOADED RESULTS** | Model `A_baseline`, N=22,266 filings, 4,103 firms, ten fiscal years, firm+year FE and firm-clustered CR1 SE; seven non-momentum controls; CAR `[-1,+1]` in percentage points. Results and contrasts verified against uploaded CSVs. |
| Stage 9 momentum A/B/C common-sample comparisons | COMPLETED / DOCUMENTED; REPORTING OPEN | Full baseline A N=22,266; restricted no-momentum B and matched with-momentum C at N=21,540 (20-day) and N=20,834 (60-to-5-day). Stage 9 output rows exist; prepare focused reporting without selecting a specification by p-value. |
| **Stage 9 event-day decomposition** | **COMPLETED SECONDARY RE-ESTIMATION / VERIFIED UPLOADED OUTPUTS** | Identical Model A N=22,266/4,103 sample, controls and FE. Day −1/0/+1 coefficient sum reproduces three-day primary regression to numerical precision. Exploratory, unadjusted day-level tests. |
| Older Stage 8G filing-time stratification | HISTORICAL EXPLORATORY, NOT A STAGE 9 RESULT | Prior timing estimates use an older sample/specification; re-estimate on final Stage 9 inputs before writing §5.5 numerical claims. |
| Manuscript | PARTIALLY COMPLETE | November 5 compiled PDF, 40 pages. §2–4 substantial; §5.1–5.4 drafted; four main-text tables. Introduction empty; §5.5–5.6 empty; Conclusion provisional. |

### Primary-sample flow (for Table 1)

| Sequential restriction | Retained | Excluded at step |
|---|---:|---:|
| Eligible consecutive annual-report pairs | 33,046 | — |
| Market/listing eligible | 32,126 | 920 |
| Estimated event-specific market model | 31,698 | 428 |
| Valid CAR `[-1,+1]` | 30,926 | 772 |
| Nonempty unchanged, revised **and** new sentiment | 22,709 | 8,217 |
| Complete Model A control/analysis variables | **22,266** | 443 |

The 8,217 loss is **relative to CAR-eligible observations**, not a classification/scoring failure. The full 33,046-pair corpus contains 8,055 genuinely empty new components; conditional sample losses overlap across the three categories.

### Primary inference, Model A (not a finding of unequal component slopes)

All slope units below are **CAR percentage points per one raw `[-1,1]` sentiment unit**, with firm-clustered standard errors:

| Estimate/test | Value | Statistical evidence |
|---|---:|---|
| Unchanged sentiment | −0.0223 | p=0.8869 |
| Revised sentiment | +0.0955 | p=0.6010 |
| **New sentiment** | **+0.2453** | **p=0.0304**; 95% CI [0.0232, 0.4674] |
| **New − unchanged** (principal two-sided contrast) | **+0.2676** | **p=0.1570**; 95% CI [−0.1030, 0.6381] |
| New − revised | +0.1497 | p=0.4720 |
| Revised − unchanged | +0.1178 | p=0.6117 |
| Joint coefficient equality | F(2,4102)=1.0415 | p=0.3530 |

One estimation-sample SD of new sentiment (0.3308) corresponds to **+0.0811 CAR percentage points (~8.1 bp)** under the Model A slope; this is a conditional association, not a causal effect. **An individually significant new-sentiment slope does not establish that new sentiment is more strongly associated with returns than unchanged or revised sentiment.** Failure to reject differences is not equivalence.

Secondary same-sample day decomposition, new-sentiment slopes: day −1 **+0.1297** (p≈0.0725); day 0 **−0.0261** (p≈0.6917); day +1 **+0.1417** (p≈0.0256). Day +1 minus day −1 **+0.0119** (p≈0.9016), not significantly different. The assigned event trading day can differ from EDINET submission calendar day, especially for after-close filings. These tests are exploratory and unadjusted for multiplicity.

**Interpretation boundary:** new relative to the preceding annual MD&A is not necessarily newly public at filing; returns in `[-1,+1]` may reflect prior earnings news or other concurrent disclosures. Do not claim causal EDINET effects or differential pricing absent direct evidence.

## 3. Decision gates and scientific design

- [x] **D1 — Primary taxonomy:** five-way-derived unchanged/revised/new; independent binary persistent/novel is secondary with separate provenance.
- [x] **D2 — Primary return window:** CAR `[-1,+1]` in percentage points; `[0,0]`, `[0,+1]` and day-specific AR are secondary timing outcomes.
- [x] **D3 — Principal hypothesis:** two-sided `H0: beta_new - beta_unchanged = 0`; report both other pairwise contrasts and joint Wald test.
- [x] **D4a — Base scoring scale and economic magnitude:** shared raw sentiment range `[-1,1]`, character-weighted narrative-unit scoring, empty components missing, Model A per-SD effect (+8.1 bp) calculated.
- [ ] **D4b — Measurement validity/sensitivity:** independent five-category accuracy assessment; consider predeclared minimum component length sensitivity (not performed as part of Stage 9 primary).
- [x] **D5a — Main estimation design:** no-momentum Model A, seven stated controls, firm+year FE, firm-clustered CR1; exact N/firms and A/B/C common-sample momentum analyses available.
- [ ] **D5b — Scientific sign-off:** document choices and lineages in a dated design memo; test focused sensitivity to component absence and extreme returns; verify canonical Git/outputs before claiming a frozen end-to-end release.
- [ ] **D6 — Multiplicity and inference policy:** designate primary contrast versus secondary contrasts and exploratory day/timing/window tests; present multiple-testing sensitivity transparently without reselecting specifications.
- [x] **D7 — Frozen NLP rule:** no rerunning production sentiment or disclosure classifiers, retuning prompts or relabeling successful observations to improve inference.

**Research freeze is OPEN** until D4b, D5b and D6 receive explicit evidence and sign-off. Stage 9 Model A is the *current primary checkpoint*, not a general declaration that every planned robustness has completed.

## 4. Remaining empirical work — prioritized

### P0 — Needed for credible interpretation

- [ ] **Reconcile Stage 8G filing-time analysis with Stage 9:** same underlying corrected market outcomes, Model A controls, and explicitly documented sample definitions. Test *interaction differences* across before/after-close groups, not subgroup significance alone. Clarify whether day −1 precedes the actual submission.
- [ ] **Re-estimate component-presence robustness on final inputs:** zero-fill genuinely absent component sentiment **only with presence flags**; report N/firms, contrasts, and change in estimand; compare against complete cases without conflating missing and neutral sentiment.
- [ ] **Analyze outcome tails/influence:** untrimmed Model A remains primary. Check 96 Model A cases with |CAR|>20 pp, including extreme observations, with predeclared trimming/winsorization/influence diagnostics; do not choose a cutoff based on significance.
- [ ] **Validate final five-category classification:** stratified independent Japanese manual labels; confusion matrix and classwise recall/precision for unchanged/revised/new/non-substantive/uncertain, especially separation of routine numeric updates from substantive revisions. The 150-sentence binary benchmark is a distinct design.
- [ ] **Formalize multiple-testing policy:** keep `beta_new-beta_unchanged` primary, explicitly separate secondary component tests and exploratory timing/robustness. Discuss uncertainty and statistical power for wide contrast intervals.

### P1 — Focused robustness and manuscript integration

- [ ] FY2018 regulatory-transition exclusion, including whether 2018 drives new/revised classifications; year FE alone do not remove differential 2018 effects.
- [ ] Narrower CAR windows `[0,0]`/`[0,+1]` with sample-consistent controls, plus sensitivity to changes in pre-filing controls and return-momentum alternatives.
- [ ] Component-size/length measures, document-length tails and raw-versus-number-normalized TF–IDF novelty as justified sensitivity checks.
- [ ] Reconcile the separate binary persistent/novel regressions (Stage 6E/6F) and the Stage 8A–8C whole-document sentiment × novelty benchmarks with corrected market-data lineage; report them as **secondary**, not Stage 9 component contrasts.
- [ ] Assess prior earnings/other announcements and the limits of identifying the first arrival of public information; distinguish this from the timestamp-based filing-time split.
- [ ] Integrate checked analyses and secondary tables/figures; document sample definitions, model provenance and exact use of multiplicity adjustments.

### P2 — Defer unless a documented validity issue demands it

New NLP models or prompts, wholesale rescoring, new disclosure-taxonomy optimization, extended return horizons, and additional non-prespecified explorations. **Avoid scope creep.**

## 5. Manuscript checklist — compiled 40-page PDF, 2026-11-05

| Section | State | Action |
|---|---|---|
| Abstract | WORKING DRAFT | Update after final sensitivity/inference; avoid calling incomplete results a complete finding. |
| 1 Introduction | EMPTY | Write last, once final results and limits are settled. |
| 2 Related Work | SUBSTANTIALLY DRAFTED | Light reference check and copyedit; preserve Paper 2 versus Paper 1 distinction. |
| 3 Data | SUBSTANTIALLY DRAFTED | Preserve event-sample audit; clearly distinguish full component distributions (§3.6) from estimation sample (§5.1). |
| 4 Methodology | SUBSTANTIALLY DRAFTED | Add five-category validation account, provenance/multiplicity details, and verify promised secondary specs reflect executed work. |
| 5.1 Descriptive Statistics | DRAFTED | Table 2 N=22,266; fix LaTeX float carrying table into §5.2. |
| 5.2 Primary Three-Component Regression | DRAFTED | Panel A of Table 3; preserve cautious coefficient interpretation. |
| 5.3 Component Difference Tests | DRAFTED | Panel B of Table 3 plus joint Wald; no duplicate table needed. |
| 5.4 Event-Day Decomposition | DRAFTED / EXPLORATORY RESULTS | Same-sample Table 4; preserve day +1 vs −1 equality test and no-causality caveat. |
| 5.5 Filing-Time Heterogeneity | EMPTY | Re-estimate Stage 9-compatible timing analysis then draft. |
| 5.6 Robustness Tests | EMPTY | Targeted presence indicators, tails, momentum, FY2018, windows, binary/whole-document secondary evidence. |
| 6 Conclusion | PROVISIONAL | Rewrite only after Results/robustness done. |
| References / Appendix | INCOMPLETE | Check missing MacKinlay (1997) and Financial Services Agency (2018) entries; appendix correlations currently numbered Table 5 instead of A1; fix reference metadata and temporary title `PAPER 2`. |

### Current main-text display plan

1. **Table 1** — sequential sample construction and attrition (§3.5).
2. **Table 2** — primary estimation-sample descriptive statistics (§5.1).
3. **Table 3** — primary coefficients (Panel A), direct comparisons (Panel B), joint Wald (§5.2–5.3).
4. **Table 4** — day −1 / 0 / +1 regressions, same Stage 9 sample (§5.4).
5. **Pending:** targeted filing-time and robustness table(s), only after verification; consider appendix for detailed grids.
6. **Appendix A1** — correlation matrix (currently numbered Table 5); add five-category validation and secondary benchmark figures/tables as warranted.

## 6. Delivery sequence (priority, not promised deadlines)

1. Record this 2026-11-05 checkpoint in **both** roadmap and `STATUS.md`; preserve earlier dated results rather than overwriting them.
2. Verify Stage 9 uploaded-source hashes against canonical Mac/NAS repository, check Git revision, and archive exact table/reporting scripts and configuration snapshots.
3. Resolve Section 5.5 with updated filing-time tests; complete presence-indicator, extreme-CAR and FY2018 sensitivity; prepare five-category validation.
4. Reconcile 20-day/60-to-5-day momentum, narrower event windows and secondary binary/whole-document benchmarks, retaining their distinct estimands.
5. Complete Section 5.6 and focused appendices; declare a primary-versus-exploratory/multiplicity policy; update research conclusions **only as evidence warrants**.
6. Draft Introduction and final Conclusion; align Abstract, references, tables and terminology; perform comprehensive methodology-versus-executed-code audit before advisor submission.

## 7. Next work session — starting order

- [ ] Check Stage 9 file hashes and Git HEAD on the canonical repository; use the documented `stage9/` outputs rather than creating a new competing regression directory.
- [ ] Prepare a Stage 9-compatible **filing-time** analysis design and exact output schema before estimation, including the same-sample interaction test.
- [ ] Identify and execute only the final-input presence-indicator and extreme-return checks necessary for §5.6; save sample and provenance diagnostics.
- [ ] Begin five-category manual validation with an independent, stratified sample and frozen adjudication instructions; do not modify production labels.
- [ ] Update status and roadmap checkboxes only on actual completion and evidence, not draft intentions.

## 8. Decision log

| Date | Decision | Evidence / rationale | Artifact / commit |
|---|---|---|---|
| 2026-10-26 | Make `PAPER2_ROADMAP.md` the delivery tracker and `STATUS.md` the research log. | Clarifies artifact roles and preserves detailed historical audit trail. | Original roadmap |
| 2026-11-02 | D1 three-component taxonomy primary; independent binary secondary. | Advisor's information-location question. | Paper 2 §4.6 |
| 2026-11-02 | D2 CAR `[-1,+1]` primary, narrower windows and daily AR secondary. | Event-study design and timing limits. | Paper 2 §4.5 |
| 2026-11-02 | D3 `beta_new-beta_unchanged` principal two-sided contrast. | Formal test is required; isolated new slope is insufficient. | Paper 2 §4.6 |
| 2026-11-02 | Exclude historical momentum from primary model; use matched-sample A/B/C robustness. | Avoid otherwise unnecessary momentum-history attrition; choice not based on p-values. | Stage 9 models A/B/C |
| **2026-11-05** | **Adopt Stage 9 Model A as current primary reported specification.** | N=22,266/4,103; new slope p=0.0304, principal contrast p=0.1570, joint p=0.3530. Seven primary controls and firm/year FE. | Stage 9 `model_summary.csv`, `coefficients.csv`, `coefficient_contrasts.csv`, `provenance.json`; generated Table 3 |
| **2026-11-05** | **Add same-sample Stage 9 event-day decomposition, exploratory.** | Day coefficients sum exactly to main CAR coefficient; day +1 minus day −1 p≈0.902. | `build_stage9_event_day_table.py`, generated Table 4/provenance |
| **2026-11-05** | Defer manuscript result freeze pending validity and sensitivity work. | Classifier validation, component-presence selection, influence/tails, filing timing, FY2018 and multiplicity remain. | This roadmap / new status checkpoint |

## 9. Provenance and update convention

- Latest compiled manuscript: `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (3).pdf` (40 pages, 2026-11-05).
- Primary Stage 9 source in repository: `data/interim/paper2/regressions/stage9/` (uploaded CSV and metadata verified for this checkpoint; repository Git state **not** verified).
- Model A sample SHA-256 (recorded by Stage 9): `df11c2d813d1e8390c2e7a7f9742241c5a750706a56325d38d1a2e508485332c`.
- Primary reporting script: `scripts/paper2/build_stage9_primary_regression_table.py`; expected generated table `stage9/tables/table3_primary_stage9.tex`.
- Descriptive reporting script: `scripts/paper2/build_stage9_descriptive_tables.py`; sample flow, descriptives and correlations under `stage9/tables/`.
- Event-day secondary estimator/reporting script: `scripts/paper2/build_stage9_event_day_table.py`; `stage9/tables/table4_event_day_decomposition.tex` and its provenance. Independent re-estimation matches Model A coefficients/SEs to floating-point precision; it **does** fit secondary day regressions and **does not** rerun primary pipeline or LLM stages.
- `STATUS.md` retains older Stage 8E/8F/8G, corrected Stage 8H/8I and all experimental histories as **dated, not necessarily current estimates**.
- Change checkboxes and claims only with output or code evidence. Clearly distinguish **FROZEN SOURCE**, **VERIFIED UPLOADED OUTPUT**, **COMPLETED HISTORICALLY**, **EXPLORATORY**, **OPEN**, and **VERIFIED IN REPO**. Never conflate source/commit verification with the existence of an uploaded report or compiled PDF.
