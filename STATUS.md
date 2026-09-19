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

The **full 37,807-filing Stage 2 extraction is currently running**. Early progress is healthy. The first observed failure was an `xbrl_not_found` case for EDINET code `E05821`, a foreign issuer, and therefore outside the intended Japanese-firm research universe. Failures will be summarized and reviewed after the batch completes rather than interrupting the production run.

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

5. **Complete and validate the Paper 2 text pipeline.**
   - Allow the full Stage 2 MD&A extraction run to finish.
   - Summarize extraction statuses, methods, text-length distributions, and failure cases.
   - Explicitly identify/exclude foreign issuers as part of sample construction.
   - Freeze the canonical MD&A corpus and extraction/QC manifest.
   - Implement text statistics, longitudinal matching, and novelty construction.

6. **Adapt the Paper 1 market-data and regression infrastructure.**
   - Reuse event-study and regression infrastructure where possible.
   - Preserve historical sample construction without filtering on current EDINET listing status.

7. **Update the Introduction and Abstract later.**
   - The current abstract still reflects the older lexical-versus-GPT framing and should not be treated as final.

## Current bottleneck

The literature review is no longer the main bottleneck.

The bottleneck has shifted to:

> **completing the empirical design while finishing and validating the full MD&A corpus, then formalizing textual novelty**

The literature review is sufficiently developed that additional reading should now be driven by specific unresolved theory or specification questions. On the coding side, Stage 2 is now implemented and running at full scale; the next bottleneck is extraction QC followed by longitudinal text matching and formal novelty construction.

## Rough completion estimate

**Overall paper:** approximately 40–45%

Approximate status by component:

- Related Work / research gap: 80–85%
- Research question / hypotheses: 65–75%
- Empirical design: 40–45%
- Data acquisition / reference-data infrastructure: 90–95%
- Coding / pipeline adaptation: 55–65%
- MD&A extraction core / batch pipeline: 90–95%
- Novelty implementation: 10–20%
- Main results: 0–10%
- Robustness tests: 0%
- Final Introduction / Abstract / Conclusion: 20–30%
- Final tables / polishing / submission readiness: 10–20%
