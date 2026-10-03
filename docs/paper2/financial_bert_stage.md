# Paper 2 — Japanese Financial BERT Stage 6B

This bundle implements the contextual sentiment stage using
`izumi-lab/bert-base-japanese-fin-additional` as the financial-language backbone.

## Methodological choice

The Izumi checkpoint is a **pretrained financial BERT**, not a sentiment classifier.
The stage therefore does not attach an arbitrary untrained three-class head at inference.
Instead it follows the closest published Japanese-finance precedent, Nakatsuka & Suimon
(2024):

1. prepare sentence-level labels from **chABSA**;
2. train **two separate binary classifiers** from the Izumi backbone:
   - positive opinion present / absent;
   - negative opinion present / absent;
3. update the new classifier and the final BERT encoder layer by default;
4. score each MD&A sentence independently with both classifiers;
5. define the primary document score as

   `bertNet = (positiveSentenceCount - negativeSentenceCount) / sentenceCount`.

A sentence can be positive, negative, both, or neither. Mean positive and negative
probabilities are also saved as secondary measures.

## Files

```text
src/mdna_analysis/financial_bert_sentiment.py
src/pipeline/stages/financial_bert_sentiment.py
src/mdna_analysis/financial_bert_plots.py
src/pipeline/stages/financial_bert_plots.py
scripts/paper2/prepare_chabsa_sentiment.py
scripts/paper2/train_financial_bert_sentiment.py
scripts/paper2/smoke_test_financial_bert.py
scripts/paper2/diagnose_financial_bert_sentiment.py
configs/paper2/financial_bert_stage.toml.txt
scripts/paper2/run_pipeline.patch.txt
tests/test_financial_bert_sentiment.py
tests/test_prepare_chabsa_sentiment.py
```

## Dependencies

The Japanese tokenizer used by the backbone requires the normal Japanese BERT stack.
A typical environment needs:

```bash
pip install torch transformers accelerate scikit-learn fugashi ipadic
```

Use the versions already pinned by the Paper 2 environment where possible rather than
blindly upgrading the research environment.

## 1. Obtain chABSA

Obtain `chakki-works/chABSA-dataset` outside Git-tracked Paper 2 outputs, for example
under a local data/scratch directory. The upstream GitHub README still points to an old S3
archive that currently returns 404. A current Kaggle mirror is available as
`takahirokubo0/chabsa`; if using the Kaggle CLI, for example:

```bash
kaggle datasets download -d takahirokubo0/chabsa -p ~/paper2_stage6b/chabsa --unzip
```

The preparation script does not depend on where the dataset came from; it expects the
standard chABSA JSON schema (`header`, `sentences`, `opinions`, `polarity`).

## 2. Prepare sentence labels

```bash
python scripts/paper2/prepare_chabsa_sentiment.py \
  --chabsa-root /path/to/chABSA-dataset \
  --output-csv data/interim/paper2/sentiment/financial_bert/chabsa_sentences.csv
```

The output contains independent `positiveLabel` and `negativeLabel` targets, plus a
four-state diagnostic label (`positive_only`, `negative_only`, `both`, `neither`).

## 3. Fine-tune both classifiers

```bash
python scripts/paper2/train_financial_bert_sentiment.py \
  --input-csv data/interim/paper2/sentiment/financial_bert/chabsa_sentences.csv \
  --output-dir models/paper2/financial_bert_sentiment
```

Literature-aligned defaults:

- 8:1:1 train/validation/test split;
- learning rate 5e-5;
- train batch size 16;
- warmup steps 100;
- weight decay 0.01;
- classifier + final BERT encoder layer trainable;
- checkpoint selected by minimum validation loss.

The published 2024 method does **not state a maximum epoch count** in its reported
hyperparameter table. The script therefore exposes `--epochs` and records it explicitly;
default is 10. This should not be described as a literature-replication parameter.

As a validation reference rather than a hard acceptance threshold, Nakatsuka & Suimon
(2024) report a 611-sentence test set with positive F1 = 0.95 and negative F1 = 0.94
(weighted F1 = 0.97 for each task). Our random split seed is explicit, so exact confusion
matrices need not reproduce theirs. Large deviations should be investigated before corpus
inference.

The final checkpoints are:

```text
models/paper2/financial_bert_sentiment/positive/final
models/paper2/financial_bert_sentiment/negative/final
```

## 4. Smoke-test corpus inference

```bash
python scripts/paper2/smoke_test_financial_bert.py \
  --manifest-csv data/interim/paper2/tokens/sudachi_c_raw/manifest.csv \
  --mdna-root ~/paper2_stage4/mdna \
  --positive-model models/paper2/financial_bert_sentiment/positive/final \
  --negative-model models/paper2/financial_bert_sentiment/negative/final \
  --output-dir data/interim/paper2/sentiment/financial_bert/smoke \
  --documents 25
```

Inspect sentence counts, positive/negative rates, `bertNet`, and unknown-token rates before
turning on the full stage.

## 5. Pipeline integration

Apply the config snippet and runner patch, then set:

```toml
financial_bert_sentiment = true
```

The production stage uses the Stage 4 `sudachi_c_raw` manifest **only to define the exact
37,473-document universe**. It reads original MD&A text and retokenizes with the model's own
MeCab/IPADIC + WordPiece tokenizer.

Inference is block-checkpointed and resumable. Overlong source sentences are split into
512-token-compatible pieces and recombined to one source-sentence probability before the
binary decisions are made; no source sentence is silently truncated.

## 6. Diagnostics

```bash
python scripts/paper2/diagnose_financial_bert_sentiment.py \
  --bert-csv data/interim/paper2/sentiment/financial_bert/financial_bert_sentiment.csv \
  --filings-csv data/interim/paper2/edinet/filings.csv \
  --lmmd-csv data/interim/paper2/sentiment/lmmd/lmmd_sentiment.csv \
  --out-dir data/interim/paper2/sentiment/financial_bert/diagnostics
```

The diagnostic explicitly tests the FY2017→FY2018 within-firm discontinuity and compares
BERT with LMMD. Given the documented 2018 disclosure reform and the LMMD break already
identified in Paper 2, this is a required validation step rather than optional plotting.

## 7. Reproducible figures

The optional `financial_bert_plots` stage writes PDF/PNG paper figures and diagnostics under
`outputs/paper2/figures/sentiment/financial_bert/`, including the overall distribution,
annual mean/median, positive/negative sentence incidence, year-by-year boxplots, and a
BERT-versus-LMMD document-level hexbin diagnostic.
