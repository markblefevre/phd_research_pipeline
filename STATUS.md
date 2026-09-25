# Paper 2 — Current Status

**Last updated:** 2026-09-25

## Current thesis

The paper asks whether greater NLP sophistication becomes more economically useful when financial disclosures contain more textually novel information.

Rather than treating the study as a simple comparison of LMMD, Japanese Financial BERT, and GPT, the paper now focuses on a conditional question:

> **Does contextual NLP add more value when financial disclosures contain genuinely new information?**

More precisely, the study asks whether the relative economic informativeness of contextual and generative sentiment measures increases as disclosure language becomes more textually novel relative to the firm’s prior report.

## What is complete

### Related Work

The Related Work section is now in coherent prose and organized around seven sections:

1. Foundations of financial sentiment measurement
2. Machine learning, contextual models, and domain adaptation
3. Generative LLMs and financial reasoning
4. Japanese financial text and closest empirical precedents
5. Information location and forward-looking disclosure
6. Textual change, novelty, and disclosure informativeness
7. Research gap

The old “Overall story arc” planning section has been removed because the narrative is now embedded in the prose.

### Research gap

The gap is no longer:

> Which sentiment model performs best on Japanese annual reports?

Okada et al. (2025) already makes that framing too weak.

The current gap is:

> **Whether the relative economic usefulness of lexical, contextual, and generative sentiment measures depends systematically on textual novelty.**

The design therefore shifts from an unconditional comparison of sentiment technologies to a conditional comparison.

### Key literature positioning

- **Loughran & McDonald / Henry & Leone:** domain-specific lexical methods remain strong benchmarks.
- **Li (2010):** information location matters; sentiment measurement and text selection are separate empirical choices.
- **Huang et al. (2023):** strong precedent for constructing sentiment independently of returns and then validating it economically.
- **Frankel et al. / Siano:** important contrast because their text measures are trained directly on market outcomes.
- **Suzuki et al. (2023):** Japanese and finance-specific language-model adaptation matters, including tokenizer adaptation.
- **Kato & Goto (2021):** Japanese annual-report tone contains information about future firm performance.
- **Okada et al. (2025):** closest Japanese empirical precedent; GPT-4o-mini and Claude 3 Haiku portfolios generate significant negative risk-adjusted alphas, while dictionary, DeBERTaV2, and Gemini measures do not.
- **Brown & Tucker / Dyer / Cohen et al.:** textual change and persistence provide the foundation for the novelty dimension.
- **Muslu et al.:** persistent text should not automatically be treated as stale or uninformative.

### Bibliography / LaTeX

- Unicode-related BibTeX problems were substantially cleaned.
- Acronyms and proper nouns are protected more consistently.
- Japanese-reference handling is cleaner.
- Related Work is compiling cleanly in Overleaf.
- The bibliography is usable, though small metadata/capitalization cleanup may still remain.


### Data acquisition and pipeline

The Paper 2 data pipeline has advanced substantially.

- **Stage 1 — EDINET acquisition:** operational and effectively complete for the current corpus.
- The canonical filing manifest now contains **37,807 Annual Securities Reports**, with **37,807 unique document IDs**, **4,448 unique EDINET codes**, and **4,448 unique securities codes**.
- A calendar-year summary initially showed **37,724 unique `edinetCode × periodEnd.year` combinations** and **83 apparent duplicate firm-years**. Stage 3 later showed that these were legitimate reporting-period transitions, mainly fiscal-year-end changes, rather than true duplicate reporting periods.
- Submission-date coverage extends from **2016-09-20 through 2026-09-18**, while fiscal-period coverage extends from **2015-07-31 through 2026-06-30**.
- The damaged/truncated filing manifest was reconstructed from the EDINET listing API, validated with the filing-summary utility, and then extended through the current scan window.
- The final manifest row count of **37,807** matches the count of downloaded non-empty raw EDINET ZIP files, providing a strong corpus-integrity cross-check.
- Stage 1 is resumable using `download_checkpoint.json`; previously downloaded non-empty ZIP files are skipped safely.
- A standalone filing-summary utility produces filing counts, yearly distributions, firm-year counts, duplicate diagnostics, missing-field diagnostics, and EDINET field distributions.
- A standalone manifest-rebuild utility now provides a safe recovery path if `filings.csv` is damaged or truncated.
- The pipeline runner has begun to be modularized: the EDINET stage was moved out of the monolithic `run_pipeline.py` into `src/pipeline/stages/edinet_download.py`.

The EDINET reference-data utilities from Paper 1 have also been migrated into the Paper 2 codebase. The current Japanese and English EDINET code lists can be downloaded reproducibly using dated snapshots, `latest` aliases, SHA-256 hashes, and a manifest. These code lists are treated as **current-state reference metadata**, not as historical point-in-time listing-status data, because using current listing status to filter historical filings would introduce survivorship bias.

### MD&A extraction

Stage 2 MD&A extraction is now implemented as a reproducible, resumable pipeline stage.

The production design uses a fast `lxml` path:

1. open the EDINET ZIP;
2. select the primary Annual Securities Report XBRL under `XBRL/PublicDoc/`;
3. locate the standardized MD&A text block by local name;
4. fall back to Japanese anchor-text matching only when necessary;
5. normalize embedded XHTML to canonical plain Japanese text.

Arelle was tested successfully as a full XBRL-aware parser and remains useful as a validation/debugging fallback, but the `lxml` approach is substantially faster and is preferred for batch extraction.

The extraction architecture is now separated into reusable components:

- `src/mdna_analysis/mdna_extraction.py` — single-filing XBRL/MD&A parser;
- `src/mdna_analysis/extract_mdna_batch.py` — reusable manifest-driven batch extractor;
- `scripts/paper2/extract_mdna_batch.py` — thin command-line wrapper;
- `src/pipeline/stages/mdna_extract.py` — Stage 2 pipeline adapter.

The batch extractor reads the canonical `filings.csv`, constructs each expected ZIP path directly without recursively scanning the NAS, writes canonical text to `data/interim/paper2/mdna/<edinetCode>/<docID>.txt`, and maintains `data/interim/paper2/mdna/extraction_manifest.csv` with extraction status, method, matched XBRL tag, text length, and error diagnostics.

A 25-filing smoke test completed successfully with **25/25 extractions**, no missing ZIPs, no parser failures, and all observations extracted through the standardized XBRL MD&A tag rather than fallback matching. Two manually inspected outputs contained the expected management discussion content and no obvious cover-page, audit-report, or unrelated-section contamination.

The refactored batch implementation was then regression-tested against the existing extraction manifest: all 25 prior successful outputs were recognized and skipped correctly, confirming resumability.

Stage 2 has now been wired into `run_pipeline.py` through the modular pipeline adapter and successfully tested through the normal pipeline entry point.

The full **37,807-filing Stage 2 extraction is now complete and QC-validated**.

Final extraction results are:

- **37,757 successful MD&A extractions** out of 37,807 filings;
- **99.8677% extraction success rate**;
- **37,752** successful extractions using the standardized primary XBRL MD&A tag;
- **4** successful extractions using the conservative Japanese anchor-text fallback;
- **1** successful manual iXBRL extraction for document `S100QGPT` (Japan Aqua, E30126);
- **50 remaining failures**.

The fallback logic was tightened after manual review showed that score-1 anchor matches could produce false positives. The production fallback now requires an anchor score of at least 3. The four surviving fallback cases were manually reviewed and found to contain genuine MD&A content.

The Japan Aqua FY2022 filing (`S100QGPT`) provided an unusual tagging case. The MD&A was clearly present in section 3 of the human-readable iXBRL HTML but was not enclosed in either of the expected standardized MD&A TextBlock concepts in the consolidated XBRL instance. Rather than weakening the general parser to accommodate a single anomalous filing, the MD&A was recovered through an explicit one-off `manual_ixbrl` extraction with provenance recorded in the extraction manifest.

Final Stage 2 QC confirms:

- Stage 1 filings rows: **37,807**
- Stage 2 manifest rows: **37,807**
- successful extractions: **37,757**
- failed extractions: **50**
- missing Stage 2 manifest rows: **0**
- extra Stage 2 manifest rows: **0**
- duplicate Stage 1 document IDs: **0**
- duplicate Stage 2 document IDs: **0**
- output-file issues among successful rows: **0**

Successful MD&A text length has a median of approximately **6,216 characters**, with the 1st and 99th percentiles at approximately **971** and **20,936** characters respectively. There are **29 short texts below 500 characters** and **37 long texts above 50,000 characters**; these are review flags rather than automatic exclusion criteria.

Stage 2 should now be treated as **complete and frozen**. Generated MD&A text files and extraction manifests are pipeline artifacts and are not committed to Git; the extraction logic, QC utility, configuration, and explicit manual-recovery script are version controlled.

### Longitudinal matching

Stage 3 — longitudinal matching — is now implemented, validated, and should be treated as **complete and frozen**.

The stage combines the canonical Stage 1 filing metadata with successful Stage 2 MD&A extractions and works directly with actual reporting-period start/end dates. An initial implementation defined firm-years using `periodEnd.year`, which incorrectly treated legitimate fiscal-year-end changes as duplicate firm-years. Manual review showed that the 83 apparent duplicate groups were typically consecutive reporting periods ending in the same calendar year, often a normal annual period followed by a shortened transition period.

The production Stage 3 logic therefore:

1. joins successful Stage 2 MD&A observations to Stage 1 filing metadata;
2. orders filings within each EDINET issuer by actual reporting period;
3. identifies only true duplicate reporting periods using `edinetCode + periodStart + periodEnd`;
4. constructs adjacent within-firm reporting-period pairs;
5. flags whether reporting periods are contiguous;
6. classifies contiguous pairs as standard annual or transition-period pairs based on reporting-period duration;
7. retains noncontiguous observations separately for audit rather than silently treating them as year-over-year pairs.

Final Stage 3 results are:

- **37,757 matched panel rows**, exactly matching the number of successful Stage 2 extractions;
- **4,441 matched EDINET codes**;
- **0 true duplicate reporting-period groups**;
- **33,316 adjacent reporting-period pairs**;
- **33,046 standard annual pairs**;
- **268 contiguous transition-period pairs**;
- **2 noncontiguous pairs**.

The two noncontiguous cases were manually investigated and found to reflect genuine issuer/listing discontinuities rather than matching failures:

- **SBI Shinsei Bank** — gap associated with its 2023 delisting;
- **Sony Financial Group** — gap associated with Sony's 2020 full acquisition / privatization and later 2025 relisting through the partial spin-off.

These checks provide strong validation that Stage 3 is identifying genuine longitudinal relationships rather than forcing observations into artificial calendar-year buckets.

Canonical Stage 3 outputs are written under:

```text
data/interim/paper2/longitudinal/
    longitudinal_panel.csv
    duplicate_reporting_periods.csv
    adjacent_period_pairs.csv
    standard_annual_pairs.csv
    transition_period_pairs.csv
    noncontiguous_pairs.csv
    summary.json
```

The baseline Stage 4 novelty analysis should begin from the **33,046 standard annual pairs**. Transition-period pairs should be retained as a diagnostic or robustness sample rather than automatically discarded.


A final Stage 3 sample-eligibility check now makes the domestic-company research universe explicit using the historical EDINET filing `formCode` attached to each filing rather than current issuer-listing metadata.

The domestic Annual Securities Report form codes used for eligibility are:

```text
030000
030200
040000
```

Foreign-company Annual Securities Reports (`formCode = 080000`) are outside the target research universe.

This rule is applied at the Stage 3 → Stage 4 boundary rather than inside the extractor. Stage 1 therefore preserves the complete filing universe, Stage 2 remains an extraction stage, and Stage 3 retains mechanical longitudinal matching before defining the research-eligible pair manifest.

The eligibility check confirms:

- **37,757 domestic matched panel rows**;
- **0 foreign matched panel rows**;
- **33,046 domestic standard annual pairs**;
- **0 foreign standard annual pairs**;
- **33,046 research-eligible pairs**.

Thus, making the domestic-company criterion explicit changes **zero observations** in the current Stage 4 baseline sample. The 50 foreign-company filings in Stage 1 are exactly the 50 Stage 2 extraction failures, so the prior sample happened already to be domestic-only; the new rule makes that property intentional and reproducible rather than dependent on parser behavior.

The canonical Stage 3 handoff to Stage 4 is now:

```text
data/interim/paper2/longitudinal/research_eligible_pairs.csv
```



### Stage 4 — token representation construction

Stage 4 has now been implemented and QC-validated through the token-representation layer. It begins from the frozen Stage 3 research-eligible pair manifest and constructs the unique document universe required for novelty measurement.

The Stage 3 research sample contains **33,046 adjacent standard annual pairs**, corresponding to **37,473 unique MD&A documents**.

Stage 4 is organized into three internal substages:

1. **4A — raw Japanese tokenization**
2. **4B — numeric normalization**
3. **4C — cross-variant quality control**

#### Stage 4A — raw tokenization

The production tokenizer uses Sudachi with NFKC normalization, natural-boundary chunking, raw numbers retained, and whitespace-only / punctuation-symbol-only tokens removed after tokenization.

Three Sudachi split modes are generated from the identical 37,473-document universe:

| Variant | Documents | Total tokens | Mean tokens/document |
|---|---:|---:|---:|
| `sudachi_a_raw` | 37,473 | 113,574,929 | 3,030.8 |
| `sudachi_b_raw` | 37,473 | 109,407,286 | 2,919.6 |
| `sudachi_c_raw` | 37,473 | 105,993,616 | 2,828.5 |

All three variants completed with **37,473/37,473 successful documents**, no duplicate `(edinetCode, docID)` keys, and no missing token files.

The expected Sudachi segmentation relationship holds for every document:

```text
tokenCount(A) >= tokenCount(B) >= tokenCount(C)
```

with **zero A<B violations** and **zero B<C violations**.

#### Stage 4B — numeric normalization

Each raw token variant is deterministically transformed into a corresponding number-normalized representation:

```text
sudachi_a_raw -> sudachi_a_num
sudachi_b_raw -> sudachi_b_num
sudachi_c_raw -> sudachi_c_num
```

The final number-normalized representations use semantic numeric normalization:

```text
numeric magnitudes        -> <NUM>
percentages               -> <NUM>%
yen-denominated amounts   -> <NUM>円
```

Japanese scale markers such as `万`, `億`, and `兆` are treated as part of numeric magnitude, while semantically meaningful mixed expressions such as `3Q`, `1人`, and `100年企業` are intentionally retained.

The transformation remains one-input-token to one-output-token, so raw and number-normalized token counts remain identical document by document.

All three numeric variants contain **37,473 documents**, all completed successfully, and raw-versus-normalized token-count mismatches are **zero** for A, B, and C.

#### Stage 4C — QC

Stage 4 now runs an explicit validator over the complete six-variant output family. The validator confirms:

- identical 37,473-document universes across all variants;
- zero duplicate document keys;
- zero missing token files;
- consistent raw-source metadata;
- zero failed or zero-token documents;
- `A >= B >= C` token-count ordering for raw and numeric variants;
- exact raw / `<NUM>` token-count equality for each Sudachi mode;
- internally valid numeric replacement counts.

The QC result is written to:

```text
data/interim/paper2/tokens/qc_summary.json
```

and currently reports:

```text
status = passed
documentCount = 37,473
```

Stage 4 token preparation should therefore now be treated as **complete and validated**.

### Stage 5 — TF-IDF / cosine textual novelty

Stage 5 is now **complete, validated, and frozen**.

The stage fits corpus-wide TF-IDF on the common **37,473-document** universe and computes adjacent-period cosine similarity / novelty for the same **33,046 research-eligible annual-report pairs** across all six Stage 4 representations.

Final novelty summaries are:

| Variant | Mean novelty | Median novelty |
|---|---:|---:|
| `sudachi_a_raw` | 0.106852 | 0.088598 |
| `sudachi_b_raw` | 0.109630 | 0.091106 |
| `sudachi_c_raw` | 0.111810 | 0.093010 |
| `sudachi_a_num` | 0.049687 | 0.034892 |
| `sudachi_b_num` | 0.051269 | 0.036312 |
| `sudachi_c_num` | 0.052434 | 0.037234 |

The Sudachi A/B/C variants are nearly identical within the raw and normalized families, with pairwise correlations around 0.997–0.999. Raw versus normalized novelty remains strongly related but meaningfully different: Pearson correlations are roughly 0.88 and Spearman correlations are around 0.71.

The primary specification is `sudachi_c_num`. The main representation robustness alternative is `sudachi_c_raw`. Sudachi A/B variants remain secondary robustness checks.

Stage 5 also produces a representation-independent `pair_diagnostics.csv` using the exact Stage 3 MD&A character counts. It contains `prevMdnaLength`, `currMdnaLength`, `lengthRatio`, `logLengthChange`, and `absLogLengthChange`.

The baseline C-num novelty measure is strongly related to absolute MD&A length change:

```text
Pearson corr(novelty, absLogLengthChange)  = 0.797
Spearman corr(novelty, absLogLengthChange) = 0.592
```

The relationship remains material after trimming extreme length changes:

| Sample | Pearson | Spearman |
|---|---:|---:|
| Full sample | 0.797 | 0.592 |
| Drop top 1% | 0.758 | 0.580 |
| Drop top 5% | 0.633 | 0.526 |
| Drop top 10% | 0.491 | 0.454 |

Source-text spot checks show that absolute-maximum novelty cases can reflect large but genuine changes in disclosure scope or structure, while observations around the 99th percentile generally contain coherent and economically meaningful textual change rather than extraction failure.

The empirical treatment is now fixed:

- keep C-num novelty intact as the main measure;
- include `absLogLengthChange` as a main control;
- use signed `logLengthChange` as a robustness specification;
- rerun key models after excluding the top 5% and top 10% of absolute length changes;
- do not residualize novelty against document length;
- do not winsorize the baseline novelty measure at this stage.

Final Stage 5 validation confirms **33,046 rows**, **0 duplicate pairs**, **0 missing diagnostic values**, and exact reproduction of all six variant summary statistics after code cleanup.

#### Stage 5 visualization and temporal diagnostics

A dedicated Stage 5 visualization/QC layer has now been added so that descriptive figures are generated reproducibly from the frozen novelty outputs rather than assembled manually. The plotting stage keeps computation separate from presentation and writes publication-oriented and diagnostic figures, together with an annual novelty summary.

The figure set includes the baseline/raw novelty distributions, C-num versus C-raw comparison, novelty by fiscal year, raw versus normalized novelty by year, novelty versus absolute log MD&A length change, and year-by-year novelty distributions.

The annual diagnostics reveal a pronounced 2018 discontinuity. For fiscal-year 2018 reports, mean C-num novelty is approximately **0.146** and the median is approximately **0.134**, compared with approximately **0.052** and **0.038** in 2019. The increase is broad-based rather than being driven only by extreme observations. Its timing coincides with the Japanese FSA narrative-disclosure reform effective for fiscal years ending on or after March 31, 2018, which reorganized MD&A-related disclosure content and increased the emphasis on management-perspective analysis. This provides institutional face validity for the novelty measure, while also identifying 2018 as a regulatory-transition year that requires explicit robustness treatment.

The current empirical treatment is to retain 2018 in the baseline with year fixed effects and rerun key specifications excluding the 2018 regulatory-transition observations. A further diagnostic should test how much of the 2018 novelty spike is explained by contemporaneous MD&A length changes and whether 2018 remains unusually novel conditional on length change.

The novelty-versus-length diagnostic is also now treated as a potential paper/appendix figure rather than only internal QC because the strong relationship directly anticipates a likely measurement concern.

#### Local SSD scratch design

Stage 4 also exposed a significant infrastructure issue: tens of thousands of small files are slow to process directly over the NAS. The canonical corpus remains on the NAS, but high-I/O stages can use a configurable local SSD scratch directory.

The Stage 4 pipeline therefore distinguishes between:

```text
canonical logical paths:
data/interim/paper2/...

physical scratch paths:
~/paper2_stage4/...
```

Manifests continue to store canonical repo-relative paths rather than machine-specific scratch paths.

This design improved Sudachi tokenization throughput dramatically. On the M1 Max, the best practical worker setting was **8 processes**:

| Workers | Throughput |
|---:|---:|
| 6 | 569.1 docs/s |
| 8 | 744.3 docs/s |
| 10 | 751.1 docs/s |

The 8-worker setting captures essentially all available speedup while avoiding unnecessary process overhead. Future stages that perform heavy small-file I/O should reuse the same canonical-NAS / local-scratch pattern.


## Current hypothesis structure

### H1 — Conditional contextual advantage

**The relative economic informativeness of contextual and generative sentiment measures, compared with a domain-specific lexical measure, increases with textual novelty.**

This is now the core hypothesis and maps directly to the research gap.

### H2 — Novelty and market relevance

**Sentiment measured in more textually novel disclosure is more strongly associated with market reactions than sentiment measured in more persistent disclosure.**

This is a supporting hypothesis rather than the main contribution.

### Secondary / robustness analyses

The following should not be headline hypotheses unless further literature development justifies them:

- year-over-year sentiment innovation
- model rankings across full / novel / persistent text
- numerical-change robustness
- alternative novelty definitions

Model-ranking changes should be treated as an empirical implication of H1 rather than as a separate hypothesis.

## Preliminary novelty sniff tests

Before formalizing Stage 4 novelty measurement, consecutive-year MD&A disclosures were examined for three large Japanese firms with very different business models: Toyota, MUFG, and Sony.

The purpose was not to establish the final novelty methodology, but to determine whether simple year-over-year textual similarity produces economically interpretable variation before committing to full-sample implementation.

A rough TF-IDF cosine similarity measure based on Japanese character 3–5-grams was applied to successive MD&A disclosures. Both raw text and a version with numerical strings normalized were examined.

Several preliminary lessons emerged:

* **Year-over-year textual novelty is clearly present and economically interpretable.** Large similarity breaks often correspond to identifiable events such as COVID-related disruption, accounting-regime changes, reporting-segment reorganizations, or major changes in business perimeter.
* **Numerical changes materially affect measured similarity.** Normalizing numbers substantially increases similarity in many firm-years, especially for highly quantitative disclosures. Raw novelty and linguistically normalized novelty therefore capture related but distinct concepts and should both be retained during methodological development.
* **Baseline textual persistence differs substantially across firms and industries.** Toyota exhibits relatively stable but visibly changing operational MD&A; MUFG shows greater annual structural and financial-statement variation; Sony contains unusually persistent accounting and valuation language, with normalized similarity often close to one.
* **Whole-document similarity can be dominated by persistent boilerplate.** Sony provides a particularly clear example: economically meaningful changes can occur within an MD&A whose large accounting-policy sections remain almost unchanged.
* **Structural disclosure changes can generate apparent novelty that is not purely economic information.** Examples include accounting-standard changes, segment reorganizations, and changes in consolidation perimeter. These cases should be diagnosed rather than automatically treated as errors.
* **Novelty and sentiment appear conceptually distinct.** Large textual changes can accompany either deterioration or improvement in business conditions, supporting the planned use of novelty as a conditioning variable rather than a directional sentiment measure.

These observations strengthen the motivation for including **industry fixed effects** in the empirical specification. They also suggest that absolute textual novelty may not be directly comparable across all industries because normal disclosure persistence appears to differ systematically by business type.

Accordingly, Stage 4 should preserve a simple absolute novelty measure as the baseline while also retaining the possibility of robustness specifications based on:

* numerical normalization;
* industry-relative novelty;
* firm-relative novelty where sufficient longitudinal history exists;
* alternative treatment of persistent versus changed portions of the MD&A;
* explicit flags for major accounting, segment, or disclosure-structure changes.

The three-firm exercise should be treated as a methodological diagnostic rather than evidence for the paper's hypotheses.

## Open empirical decisions

The main remaining design decisions are:

- whether sentence-level or changed-text analyses are needed beyond the frozen document-level baseline;
- how to define persistent versus novel portions of text in secondary analyses;
- whether grouped novelty portfolios/bins add value beyond the planned continuous interaction;
- how LMMD, Japanese Financial BERT, and GPT sentiment are normalized for comparison;
- exact market-reaction window;
- the remaining control set and fixed-effects structure;
- which robustness analyses are sufficiently informative to include in the final paper;
- whether firm- or industry-relative novelty transformations improve interpretation beyond the absolute baseline.

## Immediate next steps

1. **Stage 6A — Build the regression-ready analysis-panel foundation.**
   - Merge baseline `sudachi_c_num` novelty and `sudachi_c_raw` robustness novelty.
   - Merge `absLogLengthChange` and signed length-change diagnostics.
   - Preserve firm, reporting-period, industry, and fixed-effect identifiers.

2. **Stage 6B — Implement sentiment measurement.**
   - LMMD / domain-specific lexical measure.
   - Japanese Financial BERT / contextual measure.
   - GPT / generative measure.
   - Preserve comparable document-level outputs for each model.

3. **Write and freeze the empirical specification.**
   - Baseline sentiment effects.
   - Novelty main effect.
   - Sentiment × novelty interaction.
   - `absLogLengthChange` main control.
   - Industry and year fixed effects.
   - Length-tail and representation robustness specifications.

4. **Adapt the Paper 1 market-data and regression infrastructure.**
   - Reuse event-study and regression code where appropriate.
   - Preserve historical sample construction without filtering on current listing status.

5. **Add selected novelty robustness branches.**
   - Japanese character 3–5-gram TF-IDF.
   - Optional firm- or industry-relative novelty.
   - Tail exclusions and signed-length-change specifications.

6. **Write the formal hypothesis-development section.**
   - Develop H1 from the contextual-capacity mechanism.
   - Develop H2 from the textual-change literature.
   - State the competing interpretation that lexical methods may remain equally or more informative even in novel text.

7. **Update the Introduction and Abstract later.**
   - The current abstract still reflects the older lexical-versus-GPT framing and should not be treated as final.

## Current bottleneck

The literature review, MD&A extraction, and longitudinal matching stages are no longer the primary bottlenecks.

The bottleneck has shifted to:

> **integrating the frozen novelty specification with sentiment measures and market-reaction outcomes in a regression-ready panel**

Stages 1–5 are now complete through the baseline word-token novelty layer. The immediate empirical task is no longer novelty construction or representation selection. It is panel integration, sentiment measurement, and implementation of the main sentiment × novelty specification.

Further literature review should now be driven primarily by unresolved methodological or theoretical questions that emerge from the empirical work rather than by broad literature searching.

## Rough completion estimate

**Overall paper:** approximately 53–56%

Approximate status by component:

- Related Work / research gap: 80–85%
- Research question / hypotheses: 65–75%
- Empirical design: 60–65%
- Data acquisition / reference-data infrastructure: 95%
- Coding / pipeline adaptation: 85–90%
- MD&A extraction core / batch pipeline: 100%
- Longitudinal matching: 100%
- Novelty implementation: 90–95%
- Main results: 0–10%
- Robustness tests: 0%
- Final Introduction / Abstract / Conclusion: 20–30%
- Final tables / polishing / submission readiness: 10–20%
