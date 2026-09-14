# Paper 2 — Current Status

**Last updated:** 2026-09-14

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

5. **Adapt the Paper 1 pipeline.**
   - Reuse event-study and regression infrastructure where possible.
   - Add novelty construction and interaction terms.

6. **Update the Introduction and Abstract later.**
   - The current abstract still reflects the older lexical-versus-GPT framing and should not be treated as final.

## Current bottleneck

The literature review is no longer the main bottleneck.

The bottleneck has shifted to:

> **formal hypothesis development and empirical design**

More reading should now be driven by a specific unresolved design or theory question rather than by a general attempt to expand the literature base.

## Rough completion estimate

**Overall paper:** approximately 35–40%

Approximate status by component:

- Related Work / research gap: 80–85%
- Research question / hypotheses: 65–75%
- Empirical design: 35–40%
- Coding / pipeline adaptation: 20–30%
- Main results: 0–10%
- Robustness tests: 0%
- Final Introduction / Abstract / Conclusion: 20–30%
- Final tables / polishing / submission readiness: 10–20%
