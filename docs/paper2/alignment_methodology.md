# Paper 2 Alignment and Retrieval Methodology

**Status:** Frozen development methodology  
**Last updated:** 2026-10-06

## Purpose

The passage-level alignment stage supports the primary Paper 2 design:

> Do markets respond differently to sentiment contained in persistent disclosure language versus novel disclosure language?

The role of the alignment system is to identify the most relevant prior-year disclosure context for each current-year MD&A sentence. Alignment is deliberately separated from the later persistent-versus-novel classification step.

The retrieval system is trained and evaluated independently of market-return outcomes.

---

## Development benchmark

A manually reviewed benchmark was constructed from consecutive-year MD&A disclosures for Toyota and MUFG.

The benchmark contains:

- **150 current-year MD&A sentences**
- **129 sentences with a defensible prior-year gold counterpart**
- **21 intentional no-match cases**

Each benchmark row contains the current sentence, retrieval candidates, similarity scores, supporting context, manual persistent/novel labels, and an explicitly annotated gold prior-year sentence where a defensible counterpart exists.

The benchmark is a development and validation set rather than a production sample. It is used to compare retrieval strategies and diagnose failure modes before scaling to the full longitudinal corpus.

The frozen canonical benchmark workbook is:

```text
data/manual/paper2/alignment/
    japanese_mdna_alignment_gold_completed_top10.xlsx
```

The original benchmark sample is retained at:

```text
data/manual/paper2/alignment/
    japanese_mdna_alignment_candidates.xlsx
```

---

## Operational definition of persistence and novelty

The classification target is informational rather than lexical.

### Persistent

A current-year sentence is persistent when it conveys substantially the same economic proposition as the prior-year disclosure.

Persistence may include:

- wording changes;
- sentence splitting or merging;
- modest annual numerical updates;
- recurring definitions, labels, or accounting structures;
- repeated disclosure whose economic meaning is materially unchanged.

### Novel

A current-year sentence is novel when it introduces materially different economic information.

Novelty can arise from changes in:

- fact;
- direction;
- magnitude;
- entity;
- accounting concept;
- economic driver;
- measurement basis;
- interpretation.

Substantially revised information is therefore treated as novel rather than retained as a separate third class.

The key principle is:

> **Textual similarity is not the same as informational persistence.**

---

## Retrieval architecture

The frozen retrieval architecture is a two-stage system:

```text
Ruri Top-10 dense sentence retrieval
    ->
IDF-weighted concept containment reranking
    ->
ranked prior-year candidate set
```

The current sentence remains the classification target. Retrieved prior-year sentences provide evidence for determining whether its economic proposition is persistent or novel.

### Stage 1: dense retrieval

The primary embedding model is:

```text
cl-nagoya/ruri-v3-310m
```

For each current-year sentence, cosine similarity is computed against all prior-year sentences from the same firm, and the **Top-10** candidates are retained.

Top-10 was selected because a Top-3 pool left no room for a second-stage reranker to improve Recall@3. Expanding the pool allows reranking to promote semantically appropriate candidates that dense retrieval initially places at ranks 4-10.

Sarashina embeddings were retained as a development comparison, but Ruri produced stronger overall ranking quality.

---

## Concept-aware reranking

Dense embeddings are effective at recognizing shared reporting structure, but they can confuse economically different sentences that use nearly identical templates.

Typical examples include:

- interest received versus interest paid;
- trading transactions versus service transactions;
- domestic versus overseas disclosures;
- different credit-quality ratios;
- different subsidiaries or named entities.

To reduce these errors, the Top-10 Ruri candidates are reranked using Japanese EDINET/IFRS concept aliases.

### Corpus-derived IDF

Each matched concept alias receives an inverse-document-frequency weight derived from the full successfully extracted MD&A corpus:

```text
N = 37,757 MD&A documents
```

Concept weight is based on:

```text
idf(alias) = log((N + 1) / (df(alias) + 1)) + 1
```

where `df(alias)` is the number of MD&A documents containing that alias.

This causes common accounting language to receive little weight while rarer, more discriminating concepts receive greater weight.

### IDF-weighted containment

For current-sentence concept set `A` and candidate-sentence concept set `B`, weighted containment is:

```text
sum IDF(t), t in A ∩ B
--------------------------------
sum IDF(t), t in A
```

The selected reranking score is:

```text
score = cosine + 0.02 * IDF-weighted containment
```

The concept signal is intentionally given a small weight so that semantic similarity remains the dominant retrieval signal.

---

## Benchmark performance

Evaluation is computed only on the **129 benchmark rows with a gold prior-year counterpart**.

### Ruri cosine baseline

| Metric | Result |
|---|---:|
| Recall@1 | 0.8372 |
| Recall@3 | 0.9457 |
| Recall@10 | 0.9690 |
| MRR@10 | 0.8885 |

### Ruri + IDF-weighted containment, lambda = 0.02

| Metric | Result |
|---|---:|
| Recall@1 | **0.8527** |
| Recall@3 | **0.9612** |
| Recall@10 | **0.9690** |
| MRR@10 | **0.9050** |

The selected reranker therefore improves ranking quality without changing candidate-pool Recall@10.

On the benchmark it produces:

```text
Top-1 fixes       = 2
Top-1 regressions = 0
Net Top-1 change  = +2
```

The Ruri Top-10 candidate pool contains the gold prior-year sentence in:

```text
125 / 129 = 96.9%
```

of gold-aligned benchmark rows.

For comparison, Sarashina achieved slightly higher candidate-pool Recall@10 but weaker ranking quality overall.

---

## Context around the retrieved sentence

Three context constructions were explored:

1. best prior sentence;
2. best prior sentence plus/minus one neighboring sentence;
3. entire prior paragraph containing the best match.

The benchmark review showed that **sentence ±1** is particularly useful when economic propositions are split or merged across years.

Paragraph context can also help, but large paragraphs may contain both persistent and novel material. Paragraph context is therefore treated as supporting evidence rather than as the primary classification unit.

The current sentence remains the target unit.

---

## Important benchmark lessons

The benchmark produced several recurring methodological lessons.

### High cosine does not imply persistence

Highly templated financial disclosures can remain extremely similar even when the underlying economics change.

Examples include:

- increase versus decrease;
- profit versus loss;
- improving versus deteriorating ratios;
- different business entities inserted into the same template.

### Direction and magnitude matter

Direction reversals are strong novelty signals.

Magnitude changes can also be economically novel when the quantity is central to the proposition, even when the grammatical template is unchanged.

### Entity and accounting-concept identity matter

A one-token substitution can convert an otherwise identical sentence into a different economic proposition.

This is the main motivation for the concept-aware reranking stage.

### Retrieval and classification are separate tasks

A poor embedding match is a retrieval error, not necessarily a classification ambiguity.

The production design therefore separates:

```text
candidate retrieval
    from
persistent-versus-novel classification
```

---

## Known limitations

The retrieval system is not perfect.

Some remaining failures involve highly templated disclosures in which the economically decisive distinction is a specialized compound accounting concept not fully represented by the literal taxonomy aliases.

For example, two sentences may have almost identical structure but refer to different financial line items.

These cases demonstrate a genuine limitation of dense semantic retrieval and literal concept matching.

However, further hand-tuning of concept dictionaries or benchmark-specific rules was deliberately stopped because:

- candidate-pool Recall@10 is already approximately 97%;
- reranking improves Recall@1, Recall@3, and MRR;
- remaining failures are sparse;
- continued edge-case tuning would increase the risk of overfitting the small Toyota/MUFG development benchmark.

The retrieval/reranking stage is therefore considered frozen.

---

## Frozen production specification

The current alignment specification is:

```text
Target:
    current-year MD&A sentence

Dense retriever:
    cl-nagoya/ruri-v3-310m

Candidate pool:
    Top-10 prior-year sentences by cosine similarity

Reranker:
    IDF-weighted Japanese EDINET/IFRS concept containment

Reranker weight:
    lambda = 0.02

Supporting context:
    sentence +/- 1 where useful

Paragraph context:
    diagnostic/supporting only

Market-return information:
    never used in retrieval or classification development
```

---

## Next stage

The next methodological problem is no longer candidate retrieval.

The next stage is to determine whether the retrieved prior-year context can be used to classify each current-year sentence reproducibly as:

```text
persistent
or
novel
```

The classifier should explicitly consider:

- economic proposition;
- entity/accounting concept;
- direction;
- magnitude;
- driver;
- measurement basis;
- whether the relevant prior information spans neighboring sentences.

Once validated, sentence-level labels can be aggregated into persistent and novel text components for separate sentiment measurement and subsequent market-reaction regressions.

---

## Reproducibility scripts

The active alignment scripts are:

```text
scripts/paper2/
    build_alignment_benchmark.py
    prepare_alignment_gold_annotation.py
    retrieve_alignment_candidates.py
    evaluate_concept_reranking.py
```

Superseded exploratory code is retained under:

```text
scripts/paper2/archive/
```

Generated retrieval and evaluation artifacts belong under:

```text
data/interim/paper2/alignment/
```

while the manually curated benchmark is retained under:

```text
data/manual/paper2/alignment/
```
