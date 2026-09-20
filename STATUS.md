# Paper 2 — Current Status

**Last updated:** 2026-09-19

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
- The manifest contains **37,724 unique firm-years** and **83 duplicate firm-year observations** requiring later review.
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

Before formalizing Stage 3, consecutive-year MD&A disclosures were examined for three large Japanese firms with very different business models: Toyota, MUFG, and Sony.

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

Accordingly, Stage 3 should preserve a simple absolute novelty measure as the baseline while also retaining the possibility of robustness specifications based on:

* numerical normalization;
* industry-relative novelty;
* firm-relative novelty where sufficient longitudinal history exists;
* alternative treatment of persistent versus changed portions of the MD&A;
* explicit flags for major accounting, segment, or disclosure-structure changes.

The three-firm exercise should be treated as a methodological diagnostic rather than evidence for the paper's hypotheses.

## Open empirical decisions

The main remaining design decisions are:

- exact textual novelty measure
- whether novelty is document-level, sentence-level, or both
- how to define persistent versus novel text
- whether the main specification uses a continuous interaction or grouped novelty portfolios/bins
- how LMMD, Japanese Financial BERT, and GPT sentiment are normalized for comparison
- exact market-reaction window
- controls and fixed effects
- whether numerical changes receive separate treatment
- which analyses are core and which are robustness
- novelty may be sensitive not only to number handling and corpus-wide versus firm-specific IDF, but also to Japanese tokenization/representation choice

## Immediate next steps

1. **Write the formal hypothesis-development section.**
   - Develop H1 from the contextual-capacity mechanism.
   - Develop H2 from the textual-change literature.
   - State competing interpretation: lexical methods may remain equally or more informative even in novel text.

2. **Define the novelty variable precisely.**
   - Choose baseline similarity metric(s).
   - Decide current-report versus prior-report comparison unit.
   - Specify treatment of numbers and boilerplate.

3. **Write the empirical specification before coding.**
   - Baseline sentiment regressions.
   - Novelty main effect.
   - Sentiment × novelty interaction.
   - Relative model comparisons.

4. **Separate core tests from robustness tests.**
   - Core: LMMD / BERT / GPT + novelty interaction.
   - Secondary: persistent vs novel text, sentiment innovation, numerical-change robustness, alternative similarity measures.

5. **Construct the longitudinal MD&A panel and textual-novelty dataset.**
   - Join successful Stage 2 MD&A observations to the canonical filing metadata.
   - Order disclosures by firm and fiscal period.
   - Identify valid consecutive-year MD&A pairs.
   - Resolve the 83 duplicate firm-year observations during sample construction.
   - Produce basic text-length and longitudinal coverage diagnostics.
   - Freeze the eligible novelty-pair manifest before estimating similarity measures.

6. **Implement the baseline textual-novelty measure.**
   - Define the baseline year-over-year text representation.
   - Compute similarity between each firm's current and prior MD&A.
   - Define textual novelty as an inverse similarity measure.
   - Examine the cross-sectional and time-series distribution of novelty before introducing sentiment interactions.
   - Retain alternative similarity definitions for robustness analysis.

7. **Adapt the Paper 1 market-data and regression infrastructure.**
   - Reuse event-study and regression infrastructure where possible.
   - Preserve historical sample construction without filtering on current EDINET listing status.
   - Revisit EDINET share-count issues only if Paper 2 requires reconstructing shares outstanding, market capitalization, or per-share measures from EDINET rather than using external market data.

8. **Update the Introduction and Abstract later.**
   - The current abstract still reflects the older lexical-versus-GPT framing and should not be treated as final.

## Current bottleneck

The literature review and MD&A extraction stages are no longer the primary bottlenecks.

The bottleneck has shifted to:

> **constructing the longitudinal firm-year text panel, defining textual novelty precisely, and integrating novelty with the sentiment and market-reaction specifications**

Stage 2 extraction is complete and QC-validated. The immediate empirical task is therefore to determine the usable number of consecutive-year MD&A pairs, examine longitudinal text characteristics, and implement the baseline novelty measure.

Further literature review should now be driven primarily by unresolved methodological or theoretical questions that emerge from the empirical work rather than by broad literature searching.

## Rough completion estimate

**Overall paper:** approximately 45%

Approximate status by component:

- Related Work / research gap: 80–85%
- Research question / hypotheses: 65–75%
- Empirical design: 40–45%
- Data acquisition / reference-data infrastructure: 95%
- Coding / pipeline adaptation: 65–70%
- MD&A extraction core / batch pipeline: 100%
- Novelty implementation: 15–20%
- Main results: 0–10%
- Robustness tests: 0%
- Final Introduction / Abstract / Conclusion: 20–30%
- Final tables / polishing / submission readiness: 10–20%
