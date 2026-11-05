# Paper 2 — Research and Manuscript Roadmap

**Working document | Updated: 2026-11-05**  
**Project:** EDHEC Executive PhD in Finance — Paper 2  
**Status:** Stage 9 empirical chapter §§5.1–5.6 implemented, locally reproduced and incorporated into a 46-page manuscript; consolidated reporting code committed and pushed (Git `306a70d`). Five-category validation, whole-document benchmark reconciliation, Introduction and final Conclusion remain open.

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

## 2. Current research checkpoint — 2026-11-05 (post-robustness and Git commit)

This **supersedes the earlier November 5, November 2 and October planning checkpoints**. Prior results remain dated historical evidence in `STATUS.md`; do not promote an older Stage 8 model, restricted momentum sample or exploratory test to the primary specification.

**Verification scope:** The researcher ran `python scripts/paper2/build_stage9_tables.py` successfully on the local repository and reported both a clean staged diff check and `pytest` result (**2 passed**). Commit **`306a70d`** (`checkpoint(paper2): complete Stage 9 empirical results`) was pushed to GitHub `main`, from `690b8c0` to `306a70d`. This documents **user-confirmed local execution and remote push**, not an independent clone of the repository or a fresh end-to-end run of Stages 1–9. Source data and generated tables were not part of that six-file code commit.

| Workstream | Status | Current evidence / distinction |
|---|---|---|
| EDINET and longitudinal corpus (Stages 1–3) | COMPLETE / FROZEN | 37,807 filings; 37,757 domestic MD&A extractions; 33,046 eligible annual pairs; 37,473 distinct MD&A documents. |
| Text preparation, novelty, full-document tone (Stages 4–6D) | COMPLETE / FROZEN | Sudachi C-number-normalized primary novelty; raw-number alternative; LMMD, Japanese Financial BERT, GPT-6 Sol benchmark scores. |
| Independent binary persistent/novel (Stages 6E/6F) | COMPLETE / SEPARATE MEASURE | Frozen taxonomy; *not* a mechanical aggregation of the five-category scheme. Binary regression results now in Appendix Table 8. |
| Five-category Luna classification and three-component sentiment (Stage 8E) | COMPLETE OUTPUTS / VALIDITY OPEN | 33,046 pairs, 3,463,989 sentences, 89,946 nonempty scored components; 8,055 pairs have genuinely empty new components. Independent five-way human validation is **not yet complete**. |
| Corrected J-Quants/TOPIX event study (Stage 7) | REBUILT / DOCUMENTED | 32,126 market eligible; 31,698 fitted market models; 30,926 valid three-day CARs. No further price rebuild needed for current manuscript tables. |
| Pre-filing controls (Stage 8H) | REBUILT / DOCUMENTED | Adjusted stock-return inputs; momentum excluded from primary and assessed on common restricted samples. |
| **Stage 9 Model A** | **PRIMARY EMPIRICAL CHECKPOINT** | N=22,266, firms=4,103, fiscal years=10; seven controls; firm and year FE; firm-clustered CR1; Table 3. |
| Event-day decomposition (§5.4) | COMPLETE / EXPLORATORY | Identical Model A sample; independent estimate/SE replication; Table 4. |
| Filing-time heterogeneity (§5.5) | COMPLETE / EXPLORATORY | Pooled three-interaction tests on Model A sample; Table 5; prior Stage 8G split is historical. |
| Robustness (§5.6) | COMPLETE WORKING ANALYSES | Windows, FY2018 exclusion, tail treatments, novelty/length controls, FE alternatives, component-presence expansion; Table 6. Binary/momentum/multiplicity Tables 8–10. |
| Consolidated reporting + tests | **COMMITTED / PUSHED** | `scripts/paper2/build_stage9_tables.py`, `src/mdna_analysis/stage9.py`, `src/pipeline/stages/stage9.py`, `src/pipeline/stages/stage9_robustness.py`, `configs/paper2/stage9.toml`, `tests/test_stage9.py` at `306a70d`. |
| Manuscript | **46-PAGE WORKING DRAFT** | §§2–4 substantially drafted, §§5.1–5.6 written with six main-text tables and four appendix tables (numbered 7–10); Introduction empty, Conclusion provisional; scientific and editorial sign-off still open. |

### Sequential sample flow (Table 1)

| Restriction | Retained | Excluded |
|---|---:|---:|
| Eligible annual-report pairs | 33,046 | — |
| Market/listing eligible | 32,126 | 920 |
| Fitted market models | 31,698 | 428 |
| Valid three-day CAR `[-1,+1]` | 30,926 | 772 |
| All three substantive sentiment components present | 22,709 | 8,217 |
| Complete seven controls / **Model A sample** | **22,266** | 443 |

The 8,217 observations lost between valid CARs and the three-component complete-case stage are not an LLM failure. The robustness presence-indicator model uses **30,328 filings / 4,194 firms** with complete controls, zero-coding only genuinely absent components **together with** presence indicators; this changes sample and estimand.

### Canonical inference and economic interpretation

| Model A finding | Estimate | p-value / interpretation |
|---|---:|---|
| Unchanged sentiment | −0.0223 | p=0.8869 |
| Revised sentiment | +0.0955 | p=0.6010 |
| **New sentiment** | **+0.2453** | **p=0.0304**, CI [0.0232, 0.4674] |
| **Primary contrast: new − unchanged** | **+0.2676** | **p=0.1570**, CI [−0.1030, 0.6381] |
| New − revised | +0.1497 | p=0.4720 |
| Revised − unchanged | +0.1178 | p=0.6117 |
| Joint equality | F(2,4102)=1.0415 | p=0.3530 |

CAR units are percentage points per raw `[-1,+1]` sentiment unit. One SD of new sentiment (0.3308) implies an estimated **+0.0811 pp (8.1 bp)** association. **Neither a significant new-sentiment slope nor nonsignificant coefficients for other components proves different slopes; formal tests do not establish a stronger new-sentiment association.** Nonsignificant contrasts are not evidence of equivalence.

- **§5.4 / Table 4:** new-sentiment day −1 +0.1297 (p=0.0725), day 0 −0.0261 (p=0.6917), day +1 +0.1417 (p=0.0256); day +1 − day −1 difference +0.0119 (p=0.9016). For after-close submissions, the day −1 trading session may be the filing calendar day.
- **§5.5 / Table 5:** new-sentiment three-day slope at/before close +0.2472 versus after close +0.2426; difference −0.0046 (**p=0.985**). Joint three-component CAR filing-time interaction F(3,4102)=2.1101 (**p=0.0968**). Unchanged-sentiment CAR interaction +0.7397 (p=0.023) is **exploratory and unadjusted**.
- **§5.6 / Table 6:** new slope +0.1155 for CAR `[0,+1]` (p=0.220), −0.0261 for `[0,0]` (p=0.692), +0.2184 excluding FY2018 (p=0.064), +0.1411 excluding 96 `|CAR|>20 pp` filings (p=0.126), +0.1742 with CAR 1/99 winsorization (p=0.052), +0.2440 using raw-number novelty (p=0.031), and +0.2364 in the 30,328-filing presence-indicator sample (p=0.024). The **new-minus-unchanged contrast is not significant at 5% in these tests**.
- **Appendix:** Table 7 correlations; Table 8 independent binary persistent/novel (matched novel p=0.178; expanded p=0.500, differences nonsignificant); Table 9 momentum matched-sample A/B/C; Table 10 illustrative Holm correction (new slope raw p=0.030, adjusted p=0.091; primary contrast raw p=0.157).

**Interpretation boundary:** “New” means not present in last year's annual MD&A, not first publicly available at filing. Wide `[-1,+1]` CARs may absorb information released elsewhere; these are conditional associations rather than identified causal filing reactions. Sensitivity to tails, narrower windows, classification and multiplicity is substantive—not something to optimize away.

## 3. Research design gates and remaining scientific sign-offs

- [x] **D1** — Five-way-derived unchanged/revised/new taxonomy primary; binary persistent/novel secondary and independently constructed.
- [x] **D2** — Primary CAR `[-1,+1]` (percentage points); `[0,0]`, `[0,+1]`, and individual days secondary.
- [x] **D3** — Principal two-sided contrast `H0: β_new − β_unchanged = 0`; other pairwise tests and joint equality secondary.
- [x] **D4a** — Raw sentiment scale `[-1,+1]`, weighted component aggregation, empty components missing; primary per-SD magnitude computed.
- [ ] **D4b** — Independently validate the *actual five-category classifier* with stratified human labels, precision/recall/confusion matrix, explicit numeric-update vs substantive-revision checks; consider predeclared component-length sensitivity.
- [x] **D5a** — Model A: seven controls without momentum; firm+year FE, CR1 firm clustering, canonical sample. Tail, FY2018, windows, presence, alternative controls/FE, momentum now reported as sensitivity.
- [ ] **D5b** — Final scientific sign-off: document which robustness assumptions remain consequential, perform focused component-size/influence review, verify source/analysis lineage where required. Do not imply local reporting proves a complete end-to-end rerun.
- [x] **D6a** — Principal-versus-exploratory tests distinguished and illustrative Holm-adjusted slope/contrast families calculated (Appendix Table 10).
- [ ] **D6b** — Advisor/research sign-off on the final multiplicity and reporting policy; preserve the prespecified principal test regardless of secondary p-values.
- [x] **D7** — Production classification and LLM scores frozen. No re-running or prompt optimization to improve empirical significance.

**Stage 9 reporting and core robustness are completed; the scientific/methodology freeze is still OPEN** pending D4b, D5b, D6b and the remaining benchmark reconciliation.

## 4. Remaining work — prioritized and bounded

### P0 — Critical path toward advisor-ready draft

- [ ] **Five-category validation:** design and execute independent stratified manual adjudication of `unchanged`, `revised`, `new`, `non_substantive`, `uncertain`, including hard-to-classify numeric-update cases; report confusion matrix/class metrics and sampling protocol. The 150-sentence binary benchmark does *not* validate five-way outputs. Preserve production classifications unchanged.
- [ ] **Whole-document benchmark reconciliation:** establish whether Stage 8A–8C LMMD/BERT/GPT × novelty results used the corrected price/controls, check lineage and samples, and incorporate defensible secondary results in the manuscript. Do not mix with Stage 9 component estimands.
- [ ] **Finalize inference and validity caveats:** freeze primary versus exploratory/multiplicity policy; distinguish textual novelty from prior public-information arrival; assess whether the remaining component-length sensitivity is necessary for credibility.

### P1 — Manuscript and documentation

- [ ] Draft the **Introduction** and replace the provisional **Conclusion**; harmonize Abstract, framing and contribution against the nonsignificant component contrasts and robustness limitations.
- [ ] Resolve two visible floats from the 46-page PDF: Table 2 currently intrudes into §5.2; Table 6 appears after the Conclusion. Add strategic `\FloatBarrier` commands and **recompile/visually verify** rather than assuming success.
- [ ] Renumber appendix Tables **7–10** as **A1–A4**, or explicitly retain continuous numbering consistently; remove generic “Figures and Tables” appendix boilerplate if not used.
- [ ] Complete bibliography: check MacKinlay (1997) and FSA (2018) citations; verify closest Japanese novelty/sentiment precedent including Nakatsuka–Suimon before inclusion; clean title (`PAPER 2`) and citation metadata.
- [ ] Complete methodology-to-implementation audit, including fixed-effect nomenclature (additive industry/year versus interaction), classifier details, covariate definitions, controls, FE rank and estimator assumptions.
- [ ] Commit or independently back up **manuscript LaTeX**; it is not among the six files in Git commit `306a70d`. Update the manuscript build checkpoint once float corrections are verified.

### P2 — Only if warranted by a documented concern

Further tail thresholds, new model architectures, additional LLM scoring, extensive subgroup searching, new return horizons or taxonomy retuning. Avoid replacing a disciplined primary design with significance-searching.

## 5. Manuscript checklist — compiled 46-page PDF, 2026-11-05

Current reviewed artifact: `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (4).pdf`, **46 pages**. Tables 1–6 appear in the main body (Table 6 currently floats after §6); appendix contains Tables 7–10.

| Section / display | Current status | Next action |
|---|---|---|
| Abstract | WORKING | Ensure sensitivity / null slope differences reflected; remove “preliminary” when scientifically frozen. |
| §1 Introduction | **EMPTY** | Draft after benchmark and validity framing settled. |
| §2 Related Work | SUBSTANTIAL | Verify closest precedent and bibliography. |
| §3 Data | SUBSTANTIAL | Sample counts and extraction described; copyedit. |
| §4 Methodology | SUBSTANTIAL | Add five-way accuracy validation; reconcile promised benchmark with actual results. |
| §5.1 Descriptives / Table 2 | DRAFTED | Reposition Table 2 wholly within §5.1. |
| §5.2 Model A / Table 3 | DRAFTED | Preserve common sample and qualified slope interpretation. |
| §5.3 Coefficient equality / Table 3B | DRAFTED | Preserve principal p=0.157, joint p=0.353. |
| §5.4 Event-day / Table 4 | DRAFTED | Exploratory timing and direct day-difference test. |
| §5.5 Filing-time / Table 5 | **DRAFTED / REPRODUCED** | Interaction p=0.985 for new sentiment; exploratory unchanged interaction. |
| §5.6 Robustness / Table 6 | **DRAFTED / REPRODUCED** | Tail/window/classification sensitivity made explicit; fix float. |
| §6 Conclusion | **PROVISIONAL** | Rewrite in light of robustness and scientific validity. |
| Appendix / Tables 7–10 | INTEGRATED | Correlations, binary, momentum, Holm; prefer A-numbering. |
| References / Title | INCOMPLETE | Missing sources, metadata, working title. |

## 6. Delivery sequence (ordered work, not a promised schedule)

1. **Checkpoint completed:** Stage 9 locally regenerated Tables 1–6, plus appendices; user reported clean staged diff and 2 passing tests; code committed and pushed to GitHub `main` as `306a70d`. Manuscript compiled to 46 pages, reviewed separately.
2. **Now:** commit/update this roadmap and `STATUS.md`; keep unrelated Paper 1/temporary working-tree changes out of the Stage 9 checkpoint.
3. **Next empirical:** five-category human validation and independent whole-document benchmark lineage/reconciliation. No LLM/model rebuild by default.
4. **Next writing:** Introduction, final Conclusion, float/table and citation cleanup, then comprehensive methodological audit.
5. **Advisor readiness gate:** formal research inference review plus final source/outputs/repository and manuscript back-up confirmation.

## 7. Next work session — practical starting order

- [ ] Select a **stratified five-way validation set** and define blinded Japanese annotation/adjudication rules; audit accessible Stage 8E classifications without altering them.
- [ ] Locate historical Stage 8A–8C whole-document outputs and configurations, record corrected-market lineage and decide whether re-estimation (not rescoring) is required.
- [ ] Fix manuscript floats (`\FloatBarrier`), recompile and inspect Table 2 / Table 6 placement; check appendix numbering.
- [ ] Preserve current 46-page manuscript source in version control or a separate backup; ensure research artifacts and code commit can be associated via a clear run/provenance record.

## 8. Decision log

| Date | Decision | Evidence / rationale | Artifact / commit |
|---|---|---|---|
| 2026-10-26 | Distinguish live roadmap from historical status evidence. | Keeps current actions separate from Stage 8/legacy exploratory results. | `PAPER2_ROADMAP.md`; `STATUS.md` |
| 2026-11-02 | Three substantive components primary; binary taxonomy secondary. | Advisor-driven information-location design. | Paper 2 §§4.3, 4.6 |
| 2026-11-02 | `[-1,+1]` CAR primary and two-sided new−unchanged principal hypothesis. | Fixes inferential target before additional sensitivities. | Paper 2 §§4.5–4.6 |
| 2026-11-02 | Omit momentum from primary, assess on common eligible samples. | Avoids additional historical-return attrition. | Stage 9 A/B/C |
| 2026-11-05 | Retain Model A as primary. | N=22,266, β_new=+0.2453 (p=0.0304), new−unchanged p=0.157. | Table 3 / frozen Stage 9 outputs |
| 2026-11-05 | Incorporate event-day and filing-time diagnostics. | Same-sample Table 4; direct pooled interactions Table 5. | Consolidated builder |
| 2026-11-05 | Incorporate targeted robustness; do **not** infer unequal slopes. | Tail/window/taxonomy sensitivity; expanded presence N=30,328; principal contrasts inconclusive. | Table 6; Appendix Tables 8–10 |
| 2026-11-05 | Consolidate table execution, commit and push Stage 9 code. | User-run builder generated all tables; `pytest` 2 passed; staged diff check clean. | Git `306a70d` (`main`) |
| 2026-11-05 | Continue scientific validation before advisor-ready freeze. | Five-way labels not independently accuracy-validated; whole-document benchmark needs lineage reconciliation; Introduction/Conclusion incomplete. | This roadmap / November 5 status |

## 9. Provenance and update conventions

- Latest inspected manuscript: `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (4).pdf` (46 pages, November 5, 2026). LaTeX source was not included in Git checkpoint `306a70d`.
- Canonical analysis output directory: `data/interim/paper2/regressions/stage9/`; tables in its `tables/` subdirectory. No alternate `stage9_c` workflow should be treated as canonical.
- Consolidated reporting entry point: `scripts/paper2/build_stage9_tables.py`, with `--only robustness` and other selective targets; reuse `src/pipeline/stages/stage9_robustness.py` for estimation instead of adding one-off scripts.
- Stage 9 code commit: `306a70d` on `main`; user-provided push log confirms remote moved `690b8c0..306a70d`. Commit includes six named code/config/test files, **not** large datasets or the main LaTeX manuscript.
- Primary Model A sample SHA-256 recorded in outputs: `df11c2d813d1e8390c2e7a7f9742241c5a750706a56325d38d1a2e508485332c` (not independently rehashed in this documentation update).
- `STATUS.md` preserves dated Stage 8E/8F/8G and prior Stage 9 checkpoints as **historical**, not new competing primary findings. `PIPELINE.md` is the system/artifact map and should change only for implementation-contract changes.
- Distinguish **USER-REPORTED LOCAL RUN**, **COMMITTED/PUSHED SOURCE**, **GENERATED RESULT**, **SCIENTIFICALLY VERIFIED**, **HISTORICAL**, and **OPEN**. A passing reporting build is not proof of upstream production determinism or classification validity.
