# Paper 2: Stage 6E five-label classifier / two-group sentiment — isolated v1

**Scope:** Prepare and smoke-test a new Stage 6E pair-level classifier.
**This package makes no changes to existing source files or frozen outputs.**
No full-corpus paid API run is authorized by installing it.

## Frozen research decision

| Stage 6E label | Stage 6F sentiment grouping |
| --- | --- |
| `unchanged` | `unchanged` |
| `revised` | `changed` |
| `new_candidate` | `changed` |
| `non_substantive` | excluded |
| `uncertain` | flagged/excluded |

For the changed component, pool the **original-order sentences** classified
`revised` or `new_candidate`, then run the existing Stage 6D v2 sentiment
scorer on that pooled text. Never average the two category-level sentiment
means. A missing component remains missing, not neutral (0).

**Economic interpretation:** Changed relative to the preceding annual report,
not necessarily news to the market on the EDINET filing date.

## Install only new files

From the directory containing the extracted package:

```bash
cd ~/paper2_local
mkdir -p configs/paper2/prompts scripts/paper2
cp -n ~/Downloads/stage6e_five_to_two_v1/configs/paper2/prompts/disclosure_change_pair_classifier_5way_v1.md configs/paper2/prompts/
cp -n ~/Downloads/stage6e_five_to_two_v1/scripts/paper2/build_disclosure_change_5way_batches.py scripts/paper2/
cp -n ~/Downloads/stage6e_five_to_two_v1/scripts/paper2/run_disclosure_change_5way_smoke.py scripts/paper2/
cp -n ~/Downloads/stage6e_five_to_two_v1/scripts/paper2/collect_disclosure_change_5way.py scripts/paper2/
```

`cp -n` does not overwrite existing files. Check that copies succeeded.

## Step 1: build 3 complete-document requests, offline (no API cost)

```bash
cd ~/paper2_local
conda activate paper2
python scripts/paper2/build_disclosure_change_5way_batches.py \
  --retrieval-jsonl ~/paper2_stage4/stage6e_work/retrieval_pairs.jsonl \
  --mdna-root ~/paper2_stage4/mdna \
  --output-dir data/interim/paper2/alignment/disclosure_change_5way_v1/batch_requests_smoke
```

The builder uses the **existing original Stage 6E splitter and pair-input
builder** by importing `scripts/paper2/build_stage6e_pair_batches.py`. It
validates prior sentence count and current sentence indices. The manifest
records prompt SHA-256, original builder SHA-256, retrieval SHA-256, and model.
The retrieval JSONL is large (~8.15 GB); calculating its hash takes time.

If the MD&A folder is somewhere else, pass its real path with `--mdna-root`.

## Step 2: preview, then run 3 synchronous API requests (paid)

Preview only:

```bash
python scripts/paper2/run_disclosure_change_5way_smoke.py \
  --request-dir data/interim/paper2/alignment/disclosure_change_5way_v1/batch_requests_smoke \
  --output-dir data/interim/paper2/alignment/disclosure_change_5way_v1/smoke_responses
```

To actually call GPT-6 Luna (requires OPENAI_API_KEY):

```bash
python scripts/paper2/run_disclosure_change_5way_smoke.py \
  --request-dir data/interim/paper2/alignment/disclosure_change_5way_v1/batch_requests_smoke \
  --output-dir data/interim/paper2/alignment/disclosure_change_5way_v1/smoke_responses \
  --run
```

Successful calls are checkpointed one at a time. The same invocation can be
resumed; it skips completed IDs. An API exception stops the run and leaves
previous responses intact. **No full production batch is submitted.**

## Step 3: validate 3 responses and create five-label/two-group outputs

```bash
python scripts/paper2/collect_disclosure_change_5way.py \
  --request-dir data/interim/paper2/alignment/disclosure_change_5way_v1/batch_requests_smoke \
  --batch-output-dir data/interim/paper2/alignment/disclosure_change_5way_v1/smoke_responses \
  --retrieval-jsonl ~/paper2_stage4/stage6e_work/retrieval_pairs.jsonl \
  --output-dir data/interim/paper2/alignment/disclosure_change_5way_v1/collected_smoke
```

Outputs:
- `disclosure_change_5way_sentences.csv`: five original labels, evidence, two-group mapping.
- `disclosure_change_2group_documents.csv`: unchanged/changed texts, counts, shares;
  separate revised/new-candidate text retained for diagnostics.
- `classified_pairs_5way.jsonl`: complete five-label audit.
- `classification_failures.csv` and `classification_summary.json`.

Collector rejects missing/duplicate indices, invalid labels/confidence,
out-of-range prior indices, unexpected IDs, prompt/retrieval hash mismatches.
It will exit nonzero if any requested pair is missing or invalid. It creates
only new output directories; after a repair use a new `--output-dir`.

## Next phase — only after reviewing the smoke output

Prepare the full **33,046-pair** batch request set with `--all` and a new
`--output-dir .../batch_requests_full`, then submit through a reviewed Batch
API workflow. The script here only *builds* requests and cannot submit them.

After 100% valid coverage, run a **new isolated Stage 6F** using the existing
Stage 6D v2 tone prompt and scoring functions on `unchangedText` and
`changedText`. The existing Stage 6F-B scorer is **not directly compatible**:
it expects `persistentText` and `novelText`, and its QC is tied to the frozen
original Stage 6E universe. Do not rename columns to trick it or reuse its
cached component scores. A new scorer/QC adapter is needed.

Then build a new Stage 8 panel and run firm/year FE regressions with
`unchangedSentiment` and `changedSentiment`, controlling for text proportions
and other justified firm characteristics; use firm-clustered SE and formally
test `betaChanged = betaUnchanged`. Existing Stage 8 remains frozen.

**Validation caution:** The earlier 200-sentence five-way pilot had only top-3
prior evidence, whereas this full-pair classifier sees the complete previous
MD&A. The old 200-sentence test is exploratory and does not establish
independent accuracy of this version. We still need a bounded validation plan
before making empirical claims about classifier reliability.
