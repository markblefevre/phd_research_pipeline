# Paper 2 — Research and Manuscript Roadmap

**Working document | Updated: 2026-11-06**  
**Project:** EDHEC Executive PhD in Finance — Paper 2  
**Status:** Complete **57-page working manuscript** with §§1–6, §5.7 whole-document benchmark, Tables 1–6 and Appendix Tables A.1–A.5. Stage 9 code checkpoint `306a70d` remains the latest *reported* Git commit. Manuscript drafting is complete; independent five-way validation, benchmark data lineage, reproducibility checks, references, and final proof/sign-off remain open.

> **Purpose:** This is the *decision and delivery tracker* for finishing Paper 2. `STATUS.md` remains the detailed research log and evidence record; `PIPELINE.md` remains the implementation and artifact map. Do not overwrite or re-label frozen results as new primary findings.

## 1. Research identity and boundary with Paper 1

### Paper 1 — complete

**“Measuring Sentiment in Japanese Financial Filings.”** Compares Japanese finance-specific LMMD and GPT document-level sentiment in Nikkei 225 EDINET MD&A filings (2018–2023; 1,087 filings), assessing associations with filing-window CARs. It asks **how alternative sentiment measurement methods affect economic inference**.

### Paper 2 — current research identity

**Current manuscript title:** *Do Markets Respond to Old or New Textual Information? Evidence from Japanese Annual Reports*.

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

## 2. Current research checkpoint — 2026-11-06 (full draft; Stage 9 code checkpoint unchanged)

This **supersedes the earlier November 5 manuscript and older Stage 8 planning checkpoints**. Historical research results remain dated evidence in `STATUS.md`; do not promote a restricted momentum specification or exploratory result to the primary model. The PDF confirms manuscript content, **not** independent re-estimation or corrected-market data lineage.

**Verification scope:** The 57-page PDF `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (2)(4).pdf` was inspected November 6; manuscript completion and layout are PDF-observed. Separately, the researcher previously reported successful local execution of `python scripts/paper2/build_stage9_tables.py`, staged diff checking, 2 passing Stage 9 tests, and Git commit **`306a70d`** pushed to `main`. No new source-code or data run was performed in this documentation update, and manuscript `.tex`/`.bib` backup or commit has not been verified.

| Workstream | Status | Current evidence / distinction |
|---|---|---|
| EDINET and longitudinal corpus (Stages 1–3) | COMPLETE / FROZEN | 37,807 filings; 37,757 domestic MD&A extractions; 33,046 eligible annual pairs; 37,473 distinct MD&A documents. |
| Text preparation, novelty, full-document tone (Stages 4–6D) | COMPLETE / FROZEN | Sudachi C-number-normalized primary novelty; raw-number alternative; LMMD, Japanese Financial BERT, GPT-6 Sol benchmark scores. |
| Independent binary persistent/novel (Stages 6E/6F) | COMPLETE / SEPARATE MEASURE | Frozen taxonomy; *not* a mechanical aggregation of the five-category scheme. Binary regression results appear in Appendix Table A.2. |
| Five-category Luna classification and three-component sentiment (Stage 8E) | COMPLETE OUTPUTS / VALIDITY OPEN | 33,046 pairs, 3,463,989 sentences, 89,946 nonempty scored components; 8,055 pairs have genuinely empty new components. Independent five-way human validation is **not yet complete**. |
| Corrected J-Quants/TOPIX event study (Stage 7) | REBUILT / DOCUMENTED | 32,126 market eligible; 31,698 fitted market models; 30,926 valid three-day CARs. No further price rebuild needed for current manuscript tables. |
| Pre-filing controls (Stage 8H) | REBUILT / DOCUMENTED | Adjusted stock-return inputs; momentum excluded from primary and assessed on common restricted samples. |
| **Stage 9 Model A** | **PRIMARY EMPIRICAL CHECKPOINT** | N=22,266, firms=4,103, fiscal years=10; seven controls; firm and year FE; firm-clustered CR1; Table 3. |
| Event-day decomposition (§5.4) | COMPLETE / EXPLORATORY | Identical Model A sample; independent estimate/SE replication; Table 4. |
| Filing-time heterogeneity (§5.5) | COMPLETE / EXPLORATORY | Pooled three-interaction tests on Model A sample; Table 5; prior Stage 8G split is historical. |
| Robustness (§5.6) | COMPLETE WORKING ANALYSES | Windows, FY2018 exclusion, tail treatments, novelty/length controls, FE alternatives, component-presence expansion; Table 6. Binary/momentum/multiplicity Tables A.2–A.4. |
| Consolidated reporting + tests | **COMMITTED / PUSHED** | `scripts/paper2/build_stage9_tables.py`, `src/mdna_analysis/stage9.py`, `src/pipeline/stages/stage9.py`, `src/pipeline/stages/stage9_robustness.py`, `configs/paper2/stage9.toml`, `tests/test_stage9.py` at `306a70d`. |
| Whole-document benchmark (§5.7 / Table A.5) | INCLUDED IN MANUSCRIPT / LINEAGE OPEN | All 18 sentiment×novelty interaction specifications reported; GPT-6 Sol level interaction three-day raw p=0.00243, Holm p=0.04381; source and corrected-market lineage still requires independent check. |
| Manuscript | **57-PAGE COMPLETE WORKING DRAFT** | Title, Abstract, Introduction, Related Work, Data, Methodology, Results §§5.1–5.7, substantive Conclusion, References, and Appendices A/B present; six main tables, five appendix tables; proof and scientific sign-off open. |

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
- **Appendix A.1–A.4:** A.1 correlations; A.2 independent binary persistent/novel (matched novel p=0.178; expanded p=0.500, differences nonsignificant); A.3 common-sample momentum comparisons; A.4 illustrative Holm adjustment (new slope raw p=0.030, adjusted p=0.091; primary contrast raw p=0.157).
- **§5.7 / Appendix A.5:** Whole-document LMMD / Japanese Financial BERT / GPT-6 Sol sentiment (level and year-over-year change) × TF–IDF novelty over three CAR windows, **18 interaction tests**. Three-day GPT-6 Sol level interaction **+0.05581**, raw **p=0.00243**, Holm **p=0.04381**; BERT-level Holm **p=0.09993**. The 30,926-observation three-day benchmark uses **decimal CAR units and fiscal-year FE**, rather than Model A's 22,266 observations, pp CAR and firm+year FE. Benchmark incorporation is completed; independent data-lineage reconciliation is not.

**Interpretation boundary:** “New” means not present in last year's annual MD&A, not first publicly available at filing. Wide `[-1,+1]` CARs may absorb information released elsewhere; these are conditional associations rather than identified causal filing reactions. Sensitivity to tails, narrower windows, classification and multiplicity is substantive—not something to optimize away.

## 3. Research design gates and remaining scientific sign-offs

- [x] **D1** — Five-way-derived unchanged/revised/new taxonomy primary; binary persistent/novel secondary and independently constructed.
- [x] **D2** — Primary CAR `[-1,+1]` (percentage points); `[0,0]`, `[0,+1]`, and individual days secondary.
- [x] **D3** — Principal two-sided contrast `H0: β_new − β_unchanged = 0`; other pairwise tests and joint equality secondary.
- [x] **D4a** — Raw sentiment scale `[-1,+1]`, weighted component aggregation, empty components missing; primary per-SD magnitude computed.
- [ ] **D4b** — Independently validate the *actual five-category classifier* with stratified human labels, precision/recall/confusion matrix, explicit numeric-update vs substantive-revision checks; consider predeclared component-length sensitivity.
- [x] **D5a** — Model A: seven controls without momentum; firm+year FE, CR1 firm clustering, canonical sample. Tail, FY2018, windows, presence, alternative controls/FE, momentum now reported as sensitivity.
- [ ] **D5b** — Final scientific sign-off: document which robustness assumptions remain consequential, perform focused component-size/influence review, verify source/analysis lineage where required. Do not imply local reporting proves a complete end-to-end rerun.
- [x] **D6a** — Principal-versus-exploratory tests distinguished and illustrative Holm-adjusted slope/contrast families calculated (Appendix Table A.4).
- [ ] **D6b** — Advisor/research sign-off on the final multiplicity and reporting policy; preserve the prespecified principal test regardless of secondary p-values.
- [x] **D7** — Production classification and LLM scores frozen. No re-running or prompt optimization to improve empirical significance.

**Manuscript writing and Stage 9 reporting are completed; scientific/methodology sign-off remains OPEN** pending independent five-way validation (or an explicit retained limitation), data-lineage and measurement reproducibility checks, and final inferential/advisor review. The benchmark is **written up**, not independently source-reconciled.

## 4. Remaining work — prioritized and bounded

### P0 — Scientific credibility and advisor draft

- [ ] **Five-category validation / explicit limitation:** Independently annotate a bounded stratified set of actual `unchanged`, `revised`, `new`, `non_substantive`, and `uncertain` assignments, emphasizing numeric revisions and uncertain boundaries. Report the protocol and class metrics if feasible. If deferred, **do not** present classification accuracy as established; Appendix B.3 currently states the limitation correctly. No LLM production rescoring.
- [ ] **Benchmark lineage sign-off:** §5.7 and Appendix A.5 **are already in the manuscript**. Confirm source paths, samples, controls, and whether corrected J-Quants/TOPIX data feed the reported 18 interaction models. Do not describe this as a missing writeup or promote the benchmark into the primary component estimand. Downstream re-estimation only if an actual inconsistency is demonstrated.
- [ ] **Method-to-code consistency:** Focused check of §4.1–4.3 (Japanese LMMD resource and Sudachi variant; Financial BERT checkpoint, thresholds and document aggregation; TF–IDF construction; candidate retrieval/reranking and classification). Record any unverified assertions, preserving the frozen LLM results.
- [ ] **Scientific reporting sign-off:** Confirm principal new−unchanged contrast, exploratory tests, Holm families, tail sensitivity, event timing and sampling caveats are maintained across Abstract, Introduction, Results and Conclusion.

### P1 — Bounded manuscript polish

- [x] **Introduction and substantive Conclusion drafted** in the 57-page manuscript; empirical §§5.1–5.7 also written.
- [x] **Title, float placement, appendix numbering fixed in inspected PDF:** two-line title, Table 2 before §5.2, Table 6 before §5.7/Conclusion, Appendix Tables A.1–A.5.
- [ ] **Abstract:** replace or deliberately retain “Preliminary results indicate” based on whether this is positioned as a complete working draft; harmonize with final robustness and inconclusive contrasts.
- [ ] **References and closest precedent:** add/verify MacKinlay (1997) and Financial Services Agency (2018), both cited but absent from references; verify and consider adding Nakatsuka and Suimon (2026), *Economics Letters*, as described in `polish.md` (not yet in bibliography).
- [ ] **Remove internal pipeline identifiers:** residual reader-facing `Stage 9` phrases occur in §5 and notes to Tables 2–5; Appendix Table A.3 still uses A/B/C rows instead of descriptive labels.
- [ ] **Final citation and typesetting proof:** text/table notes, capitalization, spelling and references; consider suppressing visible red cross-reference hyperlink borders (`\hypersetup{hidelinks}`) if appropriate; recompile and inspect targeted pages after edits. No new empirical specification is implied.
- [ ] **Protect and checkpoint source:** commit or independently back up manuscript `.tex`/`.bib` and final generated tables; the historical code commit `306a70d` did not include them.

### P2 — Only if warranted by a documented concern

Further tail thresholds, new architectures, extra LLM scoring, extensive subgroup searching, new return horizons, or taxonomy retuning. Do not replace a disciplined primary design with significance-searching.

## 5. Manuscript checklist — compiled 57-page PDF, 2026-11-06

Reviewed artifact: `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (2)(4).pdf`, **57 PDF pages**. Main Tables **1–6** and Appendix Tables **A.1–A.5** are present, with the Conclusions preceding the references and appendices.

| Section / display | Current status | Next action |
|---|---|---|
| Title | COMPLETE / PDF VERIFIED | Two-line title; no generic “PAPER 2”. |
| Abstract | WORKING / PRESENT | Remove or justify “Preliminary results”; maintain qualified inference. |
| §1 Introduction | **WRITTEN / COMPLETE WORKING DRAFT** | Routine copyedit only; covers the primary contrast and benchmark separately. |
| §2 Related Work | WRITTEN | Verify closest Japanese novelty study and citations. |
| §3 Data | WRITTEN | References for FSA 2018; bounded copyedit. |
| §4 Methodology | WRITTEN / SOURCE-LEVEL CHECK OPEN | Verify reproducing LMMD/BERT/TF–IDF details and five-way validity caveat. |
| §5.1 Descriptives / Table 2 | PRESENT / FLOAT FIX VERIFIED | Table 2 now precedes §5.2 in PDF; remove internal Stage 9 notes. |
| §5.2 Model A / Table 3 | PRESENT | Preserve same sample, controls and distinct slope-vs-contrast inference. |
| §5.3 Equality tests / Table 3B | PRESENT | Primary contrast p=0.157; joint p=0.353; no proven differences. |
| §5.4 Event-day / Table 4 | PRESENT | Exploratory timing interpretation preserved. |
| §5.5 Filing-time / Table 5 | PRESENT | Interaction p=0.985 for new sentiment; exploratory subgroup tests. |
| §5.6 Robustness / Table 6 | PRESENT / FLOAT FIX VERIFIED | Table 6 now precedes §5.7 and Conclusion. |
| §5.7 Whole-document benchmark | **WRITTEN / INCORPORATED** | 18 interactions and Holm corrections reported; data lineage not independently verified. |
| §6 Conclusion | **WRITTEN / SUBSTANTIVE** | Final prose polish; integrates components, benchmark, limitations and future research. |
| Appendices A/B | PRESENT | Tables A.1–A.5 and prompts/method validity caveat; improve A.3 row labels. |
| References | INCOMPLETE | MacKinlay (1997), FSA (2018), possible Nakatsuka–Suimon (2026). |
| Provenance/source | OPEN | `.tex`/`.bib` and manuscript build/Git checkpoint not independently confirmed. |

## 6. Delivery sequence (ordered work, not a promised schedule)

1. **Completed:** Frozen component-level estimates and user-reported Stage 9 reporting-code checkpoint `306a70d`; Introduction, §5.7 benchmark writeup and Conclusion drafted; new title, corrected table floats, and A-numbering visible in the 57-page PDF.
2. **Now:** Update repository `STATUS.md`, `PAPER2_ROADMAP.md`, and `polish.md` as a *documentation-only checkpoint*; keep historical evidence intact. The attached updated documents must still be copied into the repository and committed by the researcher.
3. **Next:** Bounded source/reference edits and a method/lineage credibility pass. Human five-category validation is preferable, but an explicit limitation must remain if it cannot be completed before advisor review.
4. **Advisor-readiness gate:** Compile revised manuscript, verify references/internal terms and source backup; seek research/advisor assessment of the limits of component comparisons and benchmark interpretation.

## 7. Next work session — practical starting order

- [ ] Add the two missing cited sources and assess the closest Nakatsuka–Suimon (2026) precedent.
- [ ] Remove “Preliminary” and residual “Stage 9” references if manuscript framing warrants; relabel Appendix A.3 model rows; compile and inspect.
- [ ] Check §5.7 benchmark data lineage and the §4.1–4.3 description of implemented scoring/alignment without recomputing frozen LLM outputs.
- [ ] Resolve or document the five-way human-validation limitation.
- [ ] Back up or commit manuscript source and the documentation checkpoint separately from code results.

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
| 2026-11-06 | Accept 57-page manuscript as a **complete working draft**, not scientifically frozen. | Introduction and substantive Conclusion written; §5.7/Table A.5 included; floats and A-numbered tables corrected. | Uploaded PDF `(2)(4)`; content inspected, not source recompiled |
| 2026-11-06 | Bound remaining work to validation, lineage and proof. | Missing two cited references; unverified five-way labels; abstract “Preliminary” and reader-facing `Stage 9` remain; manuscript source commit unverified. | `STATUS.md`, roadmap and `polish.md` update |

## 9. Provenance and update conventions

- **Latest inspected manuscript (PDF):** `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (2)(4).pdf`, **57 pages**, title dated November 6, 2026. This is a **manuscript-content and placement check**, not validation of executed LaTeX source or data lineage.
- **Previous inspected manuscript (historical):** `EDHEC_LEFEVRE_PhD_Paper2_MDA_Novelty (4).pdf`, 46 pages, November 5. Do not retain its missing-section or mispositioned-table status as current.
- **Canonical Stage 9 output directory (historical code record):** `data/interim/paper2/regressions/stage9/`, tables under `tables/`; `stage9_c` is not a separate canonical workflow.
- **Consolidated reporting entry point:** `scripts/paper2/build_stage9_tables.py`, with target selection available for robustness; `src/pipeline/stages/stage9_robustness.py` is the consolidated robustness implementation.
- **Last documented code checkpoint:** Git `306a70d` on `main`, user-reported pushed from `690b8c0`. It included six code/config/test files, **not** manuscript LaTeX, large source datasets, or tables. Do not claim a new commit or end-to-end rerun based on this documentation update.
- **Stage 9 primary sample SHA-256 recorded in prior outputs:** `df11c2d813d1e8390c2e7a7f9742241c5a750706a56325d38d1a2e508485332c` (not independently rehashed here).
- `STATUS.md` retains earlier Stage 8/9 checkpoints for audit/history. `PIPELINE.md` changes only when implementation contracts change; the broad repository `README.md` is still accurate and does not require revision merely because the paper acquired a conclusion.
- Distinguish **PDF-OBSERVED CONTENT**, **USER-REPORTED LOCAL RUN**, **COMMITTED/PUSHED SOURCE**, **GENERATED RESULT**, **INDEPENDENTLY VERIFIED LINEAGE**, **SCIENTIFICALLY VALIDATED**, and **OPEN**.
