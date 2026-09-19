# Paper 2 — Current Status

**Last updated:** 2026-09-18

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

- **Stage 1 — EDINET acquisition:** operational and running successfully.
- The current filing manifest contains **35,615 Annual Securities Reports**, with **35,615 unique document IDs**, **4,422 unique EDINET codes**, and **4,422 unique securities codes**.
- The manifest currently contains **35,532 unique firm-years** and **83 duplicate firm-year observations** requiring later review.
- Submission-date coverage currently extends from **2016-09-20 through 2026-06-16**, while fiscal-period coverage extends from **2015-07-31 through 2026-03-31**.
- Stage 1 is resumable using `download_checkpoint.json`; previously downloaded non-empty ZIP files are skipped safely.
- A standalone filing-summary utility now produces filing counts, yearly distributions, firm-year counts, duplicate diagnostics, missing-field diagnostics, and EDINET field distributions.

The EDINET reference-data utilities from Paper 1 have also been migrated into the Paper 2 codebase. The current Japanese and English EDINET code lists can be downloaded reproducibly using dated snapshots, `latest` aliases, SHA-256 hashes, and a manifest. These code lists are treated as **current-state reference metadata**, not as historical point-in-time listing-status data, because using current listing status to filter historical filings would introduce survivorship bias.

### MD&A extraction

The core Stage 2 MD&A extraction logic has been recovered from Paper 1 and refactored for the new EDINET ZIP-based storage layout.

The production design now uses a fast `lxml` path:

1. open the EDINET ZIP;
2. select the primary Annual Securities Report XBRL under `XBRL/PublicDoc/`;
3. locate the standardized MD&A text block by local name;
4. fall back to Japanese anchor-text matching only when necessary;
5. normalize embedded XHTML to canonical plain Japanese text.

Arelle was tested successfully as a full XBRL-aware parser and remains useful as a validation/debugging fallback, but the `lxml` approach is substantially faster and is preferred for batch extraction.

The extractor has been validated successfully on:

- Toyota Motor, 2018 filing;
- Toyota Motor, 2021 filing;
- Sony Group, 2021 filing;
- Mitsubishi UFJ Financial Group, 2021 filing.

All four test filings were identified directly through the standardized MD&A tag, required no fallback, and extracted in approximately **0.03–0.05 seconds per filing**.

The next coding step is to wrap this validated extraction core in a resumable batch process that reads `filings.csv`, processes each ZIP, writes canonical MD&A text files, and maintains an extraction manifest with status, extraction method, text length, and error diagnostics.

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

5. **Complete the Paper 2 text pipeline.**
   - Finish the EDINET download.
   - Build the resumable batch MD&A extraction stage around the validated `lxml` extractor.
   - Write a canonical MD&A corpus and extraction/QC manifest.
   - Add novelty construction and interaction terms.

6. **Adapt the Paper 1 market-data and regression infrastructure.**
   - Reuse event-study and regression infrastructure where possible.
   - Preserve historical sample construction without filtering on current EDINET listing status.

7. **Update the Introduction and Abstract later.**
   - The current abstract still reflects the older lexical-versus-GPT framing and should not be treated as final.

## Current bottleneck

The literature review is no longer the main bottleneck.

The bottleneck has shifted to:

> **completing the empirical design while converting the validated data-extraction logic into the full batch pipeline**

The literature review is sufficiently developed that additional reading should now be driven by specific unresolved theory or specification questions. On the coding side, the highest-priority task is the Stage 2 batch MD&A extractor, followed by formal novelty construction.

## Rough completion estimate

**Overall paper:** approximately 40–45%

Approximate status by component:

- Related Work / research gap: 80–85%
- Research question / hypotheses: 65–75%
- Empirical design: 40–45%
- Data acquisition / reference-data infrastructure: 75–85%
- Coding / pipeline adaptation: 40–50%
- MD&A extraction core: 75–85%
- Novelty implementation: 10–20%
- Main results: 0–10%
- Robustness tests: 0%
- Final Introduction / Abstract / Conclusion: 20–30%
- Final tables / polishing / submission readiness: 10–20%
