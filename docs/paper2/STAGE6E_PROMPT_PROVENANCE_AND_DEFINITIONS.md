# Paper 2 — Stage 6E Prompt Provenance and Classification Definitions

**Recorded:** 2026-10-14
**Status:** Production batch prompt provenance verified; subsequent synchronous-repair provenance not independently audited.
**Scope:** Japanese annual securities report MD&A persistent/novel classification (Stage 6E).

## 1. Verified production configuration

The local prompt file and both the original production and recovery batch-build manifests identify the same prompt and model:

| Field | Verified value |
|---|---|
| Model | `gpt-6-luna` |
| Prompt path | `configs/paper2/prompts/persistent_novel_pair_classifier_v1.md` |
| Prompt SHA-256 | `0f1824dcc9477f684d3a9749880b094b2df3e83ba772204b4adcacd8605fb1b0` |
| Smoke-test requests | 10 |
| Original production requests | 33,046 |
| Recovery batch requests | 1,507 |

The smoke-test, production, and recovery manifests **all recorded the identical model, prompt path, and SHA-256 hash**. The 1,507 recovery requests are not 1,507 additional unique filing pairs.

The two local files below were also verified to have identical SHA-256 hashes:

- `configs/paper2/prompts/persistent_novel_pair_classifier_v1.md`
- `configs/paper2/prompts/persistent_novel_pair_classifier_test_v1.md`

Both matched the uploaded pair-level prompt text. Earlier sentence-level prompts (`persistent_novel_classifier_v1.md` and `persistent_novel_classifier_v1_1.md`) are distinct and **were not identified by the production batch manifest**.

### Evidence locations

The three manifest paths in `~/Downloads/pipeline_backup.tar` are:

```text
./data/interim/paper2/alignment/persistent_novel/batch_requests_smoke/batch_build_manifest.json
./data/interim/paper2/alignment/persistent_novel/batch_requests/batch_build_manifest.json
./data/interim/paper2/alignment/persistent_novel/recovery_batch_requests/batch_build_manifest.json
```

`build_stage6e_pair_batches.py` writes the `prompt_sha256` and `total_requests` fields to `batch_build_manifest.json`. `build_stage6e_recovery_batches.py` also records the prompt hash. `repair_stage6e_sync.py` defaults to the same prompt and records a hash in its audit/checkpoint data, but the hashes of the actual synchronous repair records have **not yet been individually checked**.

To recheck the original production manifest without extracting the archive:

```bash
tar -xOf ~/Downloads/pipeline_backup.tar \
  './data/interim/paper2/alignment/persistent_novel/batch_requests/batch_build_manifest.json' \
  | python -c 'import json,sys; m=json.load(sys.stdin); print({k:m.get(k) for k in ("model","prompt","prompt_sha256","total_requests")})'
```

To recheck the local prompt file:

```bash
shasum -a 256 configs/paper2/prompts/persistent_novel_pair_classifier_v1.md
```

## 2. What the production prompt actually measured

The production prompt states that the classification target is **informational, not lexical**. It compares each current-year Japanese MD&A sentence against the prior-year MD&A using a retrieval map of three candidate prior sentences, with the complete prior/current documents available as supplemental context.

### `persistent`

A sentence is persistent if it conveys substantially the same **economic or disclosure proposition** as the prior-year disclosure. The prompt explicitly permits:

- Wording changes and sentence splits or merges.
- Recurring definitions, labels, accounting structures, and disclosure templates.
- Ordinary annual numerical updates or magnitude changes **when the underlying proposition, metric, entity, and direction remain the same**.

An exact rule in the prompt is: **“A changed number alone does NOT make a sentence novel.”**

### `novel`

A sentence is novel if it introduces a new or substantively changed proposition, including:

- A sign or direction reversal.
- A different entity, subsidiary, segment, geography, product, or business.
- A different accounting concept or metric.
- A different driver or causal explanation.
- New strategic or operational information.
- A changed methodology, definition, measurement, classification, or disclosure basis.
- No defensible prior-year counterpart.

The classifier was also instructed not to infer novelty from retrieval scores and to make an independent decision for each current sentence.

**Interpretation:** Stage 6E operationalizes **propositional persistence**. It does **not** operationalize exact textual repetition, numerical invariance, or information that is demonstrably already known to investors at the filing date.

## 3. Why this distinction matters for the research question

The advisor's motivating question is whether markets respond to *old or new textual information*. The implemented Stage 6E categories do not map perfectly onto an economic notion of previously known versus newly revealed information:

1. A disclosure can be `persistent` even though its reported numerical value has changed, because the same metric, entity, and direction recur.
2. A disclosure can be `novel` because a previously discussed topic changes materially, even though the topic itself is not new.
3. A disclosure can be new relative to the **previous annual report** but already known to the market through earnings releases, financial-results briefings, or other earlier announcements.
4. Headings and structural text can be classified, but they are not necessarily economically substantive.

**Research implication (hypothesis, not established result):** Mixing routine annual updates with unchanged disclosures in the `persistent` category, and mixing revised propositions with newly introduced propositions in `novel`, may attenuate or complicate the interpretation of downstream sentiment coefficients. The validation sample alone does not establish that this explains the current regression results.

## 4. Validation context and preliminary observations

The reproducible audit sample was drawn from 3,463,989 classified sentences, using seed `20261014`:

- 100 simple-random representative sentences.
- 25 high-similarity `novel` cases.
- 25 low-similarity `persistent` cases.
- 25 lower-confidence `novel` cases.
- 25 lower-confidence `persistent` cases.

The 100 targeted observations are deliberately enriched; **do not pool them with the representative 100 to estimate population error rates**.

Files in `data/interim/paper2/alignment/persistent_novel/validation/`:

```text
stage6e_validation_sample_200_seed20261014.csv
stage6e_validation_sample_200_seed20261014_manifest.json
stage6e_validation_preliminary_review_200.csv
stage6e_validation_preliminary_findings_20261014.md
stage6e_blind_representative_100.csv
```

A preliminary, non-blind review suggested that both `persistent` and `novel` classifications can include revised information. This is consistent with the production prompt's definition and is **not automatically evidence of a model error**. Independent annotation and full-context adjudication are still needed. Current-sentence equality was verified during sample construction; the original retrieval prior-count check was not performed.

## 5. Recommended handling — decisions vs proposals

### Preserve (decision)

- Keep the existing Stage 6E classifications as the **baseline** for reproducibility.
- Preserve the exact prompt file, SHA-256, and manifest evidence with the research record.
- In any methods description, call this measure **propositional persistence/novelty relative to the prior annual report**, not proven economic-news novelty.

### Evaluate (proposals, not yet implemented)

- Independently annotate the representative sample using categories such as **unchanged**, **revised**, **new**, and **structural/non-substantive** (with **insufficient context** where necessary).
- Distinguish routine annual numerical updates from changes in economic direction, drivers, and substantive claims.
- Determine whether structural text was included in Stage 6F-B sentiment aggregation.
- Consider a sensitivity analysis with three substantive categories only if validation demonstrates that the additional distinction is reliable and economically interpretable.
- If full provenance is required, verify prompt hashes in synchronous repair checkpoint/audit JSONL records.

**Do not retroactively redefine the existing binary labels.** Any new classification scheme should be versioned and evaluated separately.

## 6. Suggested methods wording (draft; subject to advisor review)

> We classify each current-year MD&A sentence relative to the preceding annual report using a fixed LLM prompt and retrieved prior-year sentence candidates, with full-document context available for disambiguation. A sentence is classified as persistent when it expresses substantially the same economic or disclosure proposition as in the previous report, allowing stylistic changes and routine annual numerical updates that preserve the underlying metric, entity, and direction. A sentence is classified as novel when it introduces or materially changes the proposition, including changes in direction, scope, metric, causal explanation, or strategy. Consequently, our classification measures propositional novelty relative to the previous annual report rather than strict textual change or information first revealed to investors. We assess this distinction using a separately drawn manual-validation sample.

## 7. Follow-up record

- [x] Identify production pair-level prompt.
- [x] Verify local prompt SHA-256.
- [x] Verify production batch manifest: 33,046 requests, matching prompt/model/hash.
- [x] Verify recovery batch manifest: 1,507 requests, matching prompt/model/hash.
- [x] Verify smoke-test manifest: 10 requests, matching prompt/model/hash.
- [ ] Optionally verify synchronous repair checkpoint hashes.
- [ ] Complete blind human audit and adjudication.
- [ ] Verify structural-text treatment in downstream sentiment aggregation.
- [ ] Decide whether a separate revised-information measure is warranted.
