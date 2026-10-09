# Stage 6E Persistent/Novel Decomposition --- Methodology and Production Provenance

**Status:** COMPLETE / VALIDATED / FROZEN\
**Freeze date:** 2026-10-09

## 1. Purpose

Stage 6E decomposes each current-year Japanese MD&A into sentence-level
**persistent** and **novel** disclosure components so the paper can
test:

> **Do markets respond differently to sentiment contained in persistent
> disclosure language versus novel disclosure language?**

This memo records the frozen methodology, benchmark evidence,
full-corpus production architecture, recovery history, targeted
completion repair, and final validation. It is intentionally more
operational than `STATUS.md` or the pipeline overview.

## 2. Frozen Classification Definition

The production taxonomy has exactly two labels.

**Persistent:** the same underlying economic/disclosure proposition
recurs from the prior year, even if wording changes, sentences split or
merge, or routine annual numerical values update.

**Novel:** the current sentence introduces or materially changes the
proposition, including changes in direction/sign, entity, subsidiary,
segment, geography, product/business, accounting concept or metric,
driver/causal explanation, strategic/operational content, methodology,
definition, measurement, classification, or disclosure basis.

A changed numerical magnitude alone is not sufficient for novelty. The
same metric/entity/direction/proposition with updated annual values is
generally persistent. Direction reversals and substantive changes in
scope, concept, driver, or basis are novel.

## 3. Benchmark and Retrieval Freeze

The manually reviewed Toyota/MUFG benchmark contains **150 current-year
sentences**:

``` text
persistent = 83
novel      = 67
```

Frozen retrieval:

``` text
current sentence
    ↓
cl-nagoya/ruri-v3-310m top 10 prior-year sentences
    ↓
EDINET/IFRS concept + corpus-IDF reranking
score = cosine + 0.02 × weighted_jaccard
    ↓
top 3 prior-year sentences
```

On the 129 rows with a gold prior-sentence alignment:

``` text
Raw Ruri:
  Recall@1  = 0.8372
  Recall@3  = 0.9457
  Recall@10 = 0.9690
  MRR@10    = 0.8885

Ruri + corpus-IDF reranking:
  Recall@1  = 0.8450
  Recall@3  = 0.9690
  Recall@10 = 0.9690
  MRR@10    = 0.9018
```

Retrieval tuning was stopped at this point.

## 4. Classifier Architecture Freeze

The best sentence-level comparator used IDF-reranked top-3 evidence and
GPT-6 Luna:

``` text
accuracy       = 0.9600
macro-F1       = 0.9596
persistent F1  = 0.9634
novel F1       = 0.9559
errors         = 6 / 150
```

For production, the architecture was changed from one call per sentence
to one call per firm-year pair. Each pair-level request contains:

1.  complete indexed prior-year MD&A (`P0`, `P1`, ...);
2.  complete indexed current-year MD&A (`C0`, `C1`, ...);
3.  frozen top-3 retrieval mapping for every current sentence; and
4.  the pair-level classification instruction.

Retrieval scores are not sent. The top-3 map is primary evidence; full
indexed documents provide surrounding context and allow split/merge and
recurring proposition interpretation.

Two independent unchanged pair-level benchmark runs produced:

  Run                  Accuracy   Macro-F1   Persistent F1   Novel F1   Errors
  ------------------ ---------- ---------- --------------- ---------- --------
  Pair-level run 1       0.9467     0.9461          0.9518     0.9403        8
  Pair-level run 2       0.9533     0.9529          0.9576     0.9481        7

The approximately 95% pair-level accuracy was accepted because it
reduces request count from roughly **3.46 million** to **33,046** while
preserving the same frozen retrieval evidence and full document context.

## 5. Production Prompt Provenance

Production prompt:

``` text
configs/paper2/prompts/persistent_novel_pair_classifier_v1.md
```

SHA-256:

``` text
0f1824dcc9477f684d3a9749880b094b2df3e83ba772204b4adcacd8605fb1b0
```

The benchmark/test pair prompt and production pair prompt were verified
byte-identical:

``` text
persistent_novel_pair_classifier_test_v1.md
persistent_novel_pair_classifier_v1.md
```

Both have the SHA above.

Expected response schema:

``` json
{
  "classifications": [
    {
      "current_index": 0,
      "label": "persistent",
      "confidence": "high",
      "reason": "..."
    }
  ]
}
```

Allowed labels: `persistent`, `novel`.\
Allowed confidence: `high`, `mid`, `low`.

## 6. Full-Corpus Retrieval

The frozen retrieval pass covers all **33,046** research-eligible
annual-report pairs. The canonical retrieval artifact is:

``` text
data/interim/paper2/alignment/persistent_novel/retrieval_pairs.jsonl
```

Working SSD copy:

``` text
~/paper2_stage4/stage6e_work/retrieval_pairs.jsonl
```

The retrieval file is approximately 8.15 GB and is a generated artifact
rather than a Git candidate.

## 7. Full-Corpus Luna Production

Production used one GPT-6 Luna response per firm-year pair, for
**33,046** responses total.

The collector validates exact sentence-index coverage. It rejects
duplicate indices, invalid labels/confidence values, and any mismatch
between returned and expected `current_index` sets.

After the initial production run, a recovery batch was used for
incomplete responses. Recovery results were merged by `custom_id` into
one consolidated 33,046-response file, replacing the corresponding
original response rather than loading duplicate pair responses into the
collector.

After recovery, **341** documents still failed coverage validation.
Inspection showed that the responses were parseable and the remaining
failure mechanism was omitted classifications, not malformed output.

## 8. Why 429 Became 447

An early diagnostic counted **429** missing classifications from the
human-readable collector failure messages.

The collector formats coverage errors using only the first ten missing
indices:

``` python
sorted(expected - set(ans))[:10]
```

Three failed documents displayed exactly ten missing IDs:

``` text
S100LQYI
S100TTBB
S100W8AE
```

Therefore the failure CSV could not reveal their complete missing sets.

The synchronous repair utility instead computed the full set difference
between the authoritative expected indices in `retrieval_pairs.jsonl`
and the indices actually present in each response. This established the
definitive total:

``` text
failed pairs                 = 341
missing classifications      = 447
```

The earlier 429 figure is superseded.

## 9. Targeted Synchronous Completion Repair

A synchronous repair was chosen instead of another Batch API run so
failures could be observed and corrected immediately and successful work
could be checkpointed pair by pair.

Repair invariants:

-   model remains GPT-6 Luna;
-   production prompt SHA is hard-checked;
-   frozen retrieval is unchanged;
-   full indexed prior/current MD&A and retrieval map remain available;
-   only missing `C` indices are requested in the repair output;
-   existing classifications are never regenerated or overwritten;
-   unexpected or duplicate returned indices are errors;
-   returned indices must exactly equal the requested missing set;
-   full expected coverage is revalidated after merge;
-   every successful repair is immediately appended to an
    audit/checkpoint file.

Repair utility:

``` text
scripts/paper2/repair_stage6e_sync.py
```

Audit/checkpoint:

``` text
data/interim/paper2/alignment/persistent_novel/sync_repair/
    stage6e_sync_repair_audit.jsonl
```

Each audit record includes at least:

-   current and prior document IDs;
-   model;
-   production prompt path and SHA-256;
-   requested missing indices;
-   returned classifications;
-   synchronous response ID;
-   timestamp;
-   token usage;
-   attempt number;
-   post-merge coverage result.

A one-pair smoke test repaired `S100BD8C / C48` and passed post-merge
coverage before the remaining repair set was released.

## 10. Final Repair Result

The completed synchronous repair reported:

``` text
FINAL REPAIR VALIDATION PASSED.
Source responses          : 33,046
Repaired pairs            : 341
Repaired classifications  : 447
Fully covered pairs       : 33,046
```

Canonical repaired consolidated response file:

``` text
data/interim/paper2/alignment/persistent_novel/sync_repair/
    stage6e_luna_final_repaired_output.jsonl
```

The 447 classifications represent approximately 0.013% of the final
3,463,989 sentence classifications and are separately auditable.

## 11. Final Independent Collector Validation

The repaired consolidated response file was copied alone into a clean
input directory and passed through the **original production collector
unchanged**.

Final collector output:

``` json
{
  "pairs_assembled": 33046,
  "sentences_assembled": 3463989,
  "failure_rows": 0,
  "successful_batch_responses_loaded": 33046
}
```

The collector additionally reported:

``` text
All retrieved pairs assembled with no classification failures.
```

Validated assembled outputs are under:

``` text
data/interim/paper2/alignment/persistent_novel/results_final_repaired/
```

This unchanged-collector pass is the final Stage 6E QC gate.

## 12. Freeze Decision

Stage 6E is:

> **COMPLETE / VALIDATED / FROZEN**

Do not reopen:

-   persistent/novel taxonomy;
-   Ruri retrieval;
-   corpus-IDF reranking;
-   prompt tuning;
-   pair-level grouping architecture;
-   benchmark optimization;
-   already successful classifications;
-   the 447 targeted repairs,

unless a genuine data-integrity problem is discovered.

## 13. Git / Artifact Policy

Version-control the code, prompt/configuration, documentation, and small
provenance/QC artifacts needed to reproduce or understand Stage 6E.

Large generated artifacts should remain outside Git, including the 8.15
GB retrieval JSONL, raw/consolidated API response JSONLs, and
multi-million-row assembled sentence outputs. Avoid committing duplicate
staging copies such as `repaired_batch_outputs/`.

The exact final Git inclusion list can be decided after checking
artifact sizes, but `scripts/paper2/repair_stage6e_sync.py` and this
memo should be retained.

## 14. Next Stage

Stage 6F should:

1.  assemble document-level persistent and novel text components from
    the frozen Stage 6E sentence labels;
2.  define component-length and normalization treatment before scoring;
3.  score persistent and novel components separately;
4.  construct component sentiment levels and year-over-year changes;
5.  estimate persistent and novel sentiment jointly in the
    market-reaction regressions; and
6.  formally test whether their coefficients differ.

The whole-document Stage 8A--8C analysis remains frozen as the benchmark
checkpoint.
