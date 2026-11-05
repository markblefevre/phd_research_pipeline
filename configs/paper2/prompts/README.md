# Paper 2 — LLM prompt provenance

**Documentation checkpoint:** 2026-11-05  
**Scope:** `configs/paper2/prompts/`  
**Purpose:** Identify the prompt instructions associated with Paper 2 production analyses, distinguish them from exploratory versions, and preserve the exact hashing conventions. This file is an **internal reproducibility record**, not manuscript text.

## Production prompt inventory

| Research task | Prompt instruction file | Model | Provenance status |
| --- | --- | --- | --- |
| Five-category year-over-year MD&A sentence classification | `disclosure_change_pair_classifier_5way_v2.md` | `gpt-6-luna` | **Verified:** hash matches final collection summary and original batch request instructions. |
| Sentiment of unchanged, revised, and new MD&A components | `llm_sentiment_v2.md` | `gpt-6-luna` | **Verified:** script enforces normalized prompt hash; live-run manifest confirms model, hash, and 33,046 pairs. |
| Independent binary persistent/novel classification | `persistent_novel_pair_classifier_v1.md` | `gpt-6-luna` | **Verified:** production batch manifest records model and prompt hash; final repaired classification summary reports complete coverage. |
| Whole-document GPT sentiment benchmark | `llm_sentiment_v2.md` | `gpt-6-sol` | **Verified:** live-run metadata records model, normalized prompt hash, and 37,473 documents. |

The binary classifier is **not** a collapse of the five-category labels. The literal production label `new_candidate` is described as **new** in the manuscript. The five-category results are the source of the unchanged/revised/new text components used in the three-component sentiment analysis.

## Recorded SHA-256 values

| Prompt file | Checksum basis | SHA-256 |
| --- | --- | --- |
| `disclosure_change_pair_classifier_5way_v2.md` | Raw file bytes (`shasum -a 256`) | `dda8bc260b7c2054ccf4e5822e78e7509d43379dd3d7d964edaa8ff08adc319f` |
| `persistent_novel_pair_classifier_v1.md` | Raw file bytes (`shasum -a 256`) | `0f1824dcc9477f684d3a9749880b094b2df3e83ba772204b4adcacd8605fb1b0` |
| `llm_sentiment_v2.md` | Raw file bytes (`shasum -a 256`) | `7ea7cc64cc0f048bc4e0dfddf1ad08ec768164a8532a7dcd7c1491e0505c70d6` |
| `llm_sentiment_v2.md` | UTF-8 file read, `.strip()`, UTF-8 encode, then SHA-256 | `326e4698aed9575f16e26ed33e097fc0c3d4f68be1e6c774672fcd013e4801ff` |

### Why the two sentiment prompt hashes differ

`llm_sentiment_v2.md` ends with a newline. The Stage 8E production scorer executes:

```python
prompt = args.prompt.read_text(encoding="utf-8").strip()
prompt_hash = sha(prompt)
if prompt_hash != PRODUCTION_PROMPT_HASH:
    raise ValueError(...)
```

Thus the **raw file** hashes to `7ea7cc64...`, while the **text actually loaded and hashed by the scorer** hashes to `326e4698...`. Both hashes were experimentally reproduced from the current file, with removal of the last newline sufficient to obtain the recorded hash. The difference is a trailing newline, **not a substantive revision**. Keep the prompt file unchanged; do not rerun scoring to reconcile hashes. `strip()` is the implementation convention, although this particular reconciliation required only removing one final newline.

To recheck locally from the repository root:

```bash
shasum -a 256 configs/paper2/prompts/llm_sentiment_v2.md
python - <<'PY'
from pathlib import Path
from hashlib import sha256
p = Path('configs/paper2/prompts/llm_sentiment_v2.md')
text = p.read_text(encoding='utf-8').strip()
print('Normalized prompt SHA-256:', sha256(text.encode('utf-8')).hexdigest())
PY
```

## Production evidence and distinguishing details

**Five-category classifier.** The final summary at
`data/interim/paper2/alignment/disclosure_change_5way_v1/collected_repaired_prompt_v2/classification_summary.json`
reports 33,046 valid filing pairs, 3,463,989 classified current-year sentences, zero failed pairs, and the exact `dda8bc...` prompt hash. The original requests are at `batch_requests_full_prompt_v2/disclosure_change_5way_*.jsonl` below the same five-way directory. A request supplies the full indexed preceding and current MD&A and candidate retrieval correspondences; the `.md` hash identifies the **instruction file**, not the entire per-pair request payload. The initial full collection had 145 invalid pairs; the final repaired collection has zero failures. Use the **repaired** collection for analyses and validation.

**Three-component sentiment.** `scripts/paper2/run_stage8e_three_component_sentiment.py` defaults to `llm_sentiment_v2.md`, reads it with `.strip()`, and enforces `PRODUCTION_PROMPT_HASH` = `326e4698...`. The live-run manifest is at
`data/interim/paper2/sentiment/three_component_stage8e/runs/20261019T021943726283Z_4f2fbd28/manifest.json`.
It records `gpt-6-luna`, `reasoning_effort=none`, `dry_run=false`, 33,046 documents, target unit length 1,050 characters, and maximum unit length 1,400 characters. The component inputs are `unchangedText`, `revisedText`, and `newCandidateText`; the historical source filename `disclosure_change_2group_documents.csv` **does not mean** the independent binary classifier was used.

**Independent binary classifier.** The raw hashes of `persistent_novel_pair_classifier_v1.md` and `persistent_novel_pair_classifier_test_v1.md` are identical (`0f1824...`). They are byte-identical files, not evidence of two different prompt specifications. The production batch manifest at
`data/interim/paper2/alignment/persistent_novel/batch_requests/batch_build_manifest.json`
records `model=gpt-6-luna`, `prompt=configs/paper2/prompts/persistent_novel_pair_classifier_v1.md`,
`prompt_sha256=0f1824dcc9477f684d3a9749880b094b2df3e83ba772204b4adcacd8605fb1b0`,
and `total_requests=33046` across eight parts. The repaired final collection summary at
`data/interim/paper2/alignment/persistent_novel/results_final_repaired/pair_level_classification_summary.json`
records 33,046 assembled pairs, 3,463,989 assembled sentences, and zero failures. The final
summary does not itself contain a prompt hash; the model/prompt identification comes from the
original production batch manifest, while the final summary establishes collection completeness.

**Whole-document sentiment.** Research records identify `llm_sentiment_v2.md` and `gpt-6-sol` as the whole-document GPT benchmark. The actual production metadata at
`data/interim/paper2/sentiment/llm/llm_sentiment.metadata.json`
record `model=gpt-6-sol`, `reasoning_effort=none`, `dry_run=false`,
`prompt_sha256=326e4698aed9575f16e26ed33e097fc0c3d4f68be1e6c774672fcd013e4801ff`,
`document_count=37473`, `target_unit_chars=1050`, `max_unit_chars=1400`,
`min_narrative_line_chars=20`, and non-whitespace narrative-character-weighted aggregation.
The metadata record repository commit `b7fdb615f211e611db4cfede4edf9375b612d6e2`.
The recorded prompt hash matches the normalized `llm_sentiment_v2.md` text. These are
production-run metadata, not a separately executed recheck of all per-document raw response files.

## Other prompt files in this directory

The following are **not identified as the final production instruction files in the inventory above** and must not be silently substituted for them:

- `disclosure_change_pair_classifier_5way_v1.md`
- `llm_sentiment_v1.md`
- `persistent_novel_classifier_v1.md`
- `persistent_novel_classifier_v1_1.md`
- `persistent_novel_pair_classifier_test_v1.md` (byte-identical to the production binary pair prompt)
- `persistent_novel_simple_200_v1.md`

Keep these files for pilot history and provenance unless a separate repository cleanup review establishes otherwise.

## Research-integrity boundaries

- Do not modify frozen production prompt instructions or rerun expensive LLM classification/scoring merely for documentation consistency.
- Prompt identity and complete coverage do **not** establish five-category classification accuracy. Independent blinded human evaluation remains outstanding.
- The manuscript should summarize the **classification rules, model tasks, and sentiment construction**. Keep hashes, paths, normalization details, batch recovery, and run IDs in this repository record rather than the reader-facing appendix.
- If a prompt file changes in the future, archive a distinct version and record both the file checksum and the exact run-produced prompt hash; do not overwrite this historical mapping.

## Provenance check status and remaining publication preparation

All four research applications have now been checked against the available production
summary or manifest records: five-category classification, three-component sentiment,
independent binary classification, and whole-document sentiment. The binary manifest and
whole-document metadata were directly inspected on 2026-11-05. Production output completeness
was verified separately where summary counts were available; these checks are not
independent accuracy validation.

If publishing reproducibility material, archive the exact production `.md` files and the
versions of the source code that constructed the complete API requests. Consider storing a
portable snapshot of the four production metadata/summary records, which currently contain
machine-specific absolute file paths. Human validation of five-category labels remains open.
