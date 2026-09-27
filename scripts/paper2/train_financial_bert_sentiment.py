#!/usr/bin/env python3
"""Fine-tune dual binary Japanese Financial BERT sentiment classifiers.

The implementation follows the core design reported by Nakatsuka & Suimon
(2024):

* backbone: izumi-lab/bert-base-japanese-fin-additional
* data: chABSA sentences
* separate positive-presence and negative-presence binary classifiers
* train/validation/test = 8:1:1
* learning rate = 5e-5
* train batch size = 16
* warmup steps = 100
* weight decay = 0.01
* update the added binary classifier and final BERT encoder layer only
* select the checkpoint with minimum validation loss

The paper does not state a maximum epoch count in its published method table,
so ``--epochs`` remains an explicit implementation choice (default 10) and is
recorded in metadata rather than presented as a literature-specified value.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import random
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BACKBONE = "izumi-lab/bert-base-japanese-fin-additional"
ID2LABEL = {0: "absent", 1: "present"}
LABEL2ID = {v: k for k, v in ID2LABEL.items()}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input-csv", required=True, type=Path)
    p.add_argument("--output-dir", required=True, type=Path)
    p.add_argument("--backbone", default=BACKBONE)
    p.add_argument("--text-col", default="text")
    p.add_argument("--positive-label-col", default="positiveLabel")
    p.add_argument("--negative-label-col", default="negativeLabel")
    p.add_argument("--max-length", type=int, default=512)
    p.add_argument("--epochs", type=float, default=10.0,
                   help="Implementation max epochs; not reported in the 2024 paper")
    p.add_argument("--learning-rate", type=float, default=5e-5)
    p.add_argument("--weight-decay", type=float, default=0.01)
    p.add_argument("--warmup-steps", type=int, default=100)
    p.add_argument("--train-batch-size", type=int, default=16)
    p.add_argument("--eval-batch-size", type=int, default=32)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--split-mode", choices=["joint_stratified", "random"], default="joint_stratified",
        help="8:1:1 sentence split. joint_stratified stabilizes the four positive/negative states.",
    )
    p.add_argument(
        "--full-finetune", action="store_true",
        help="Robustness option: train all BERT layers instead of only final layer + classifier.",
    )
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def make_splits(df: pd.DataFrame, *, seed: int, mode: str):
    from sklearn.model_selection import train_test_split

    stratify = df["jointLabel"] if mode == "joint_stratified" else None
    train, hold = train_test_split(
        df, test_size=0.20, random_state=seed, shuffle=True, stratify=stratify
    )
    hold_stratify = hold["jointLabel"] if mode == "joint_stratified" else None
    validation, test = train_test_split(
        hold, test_size=0.50, random_state=seed + 1, shuffle=True, stratify=hold_stratify
    )
    return train.copy(), validation.copy(), test.copy()


class EncodedDataset:
    def __init__(self, encodings, labels, torch):
        self.encodings = encodings
        self.labels = list(map(int, labels))
        self.torch = torch

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        item = {k: self.torch.tensor(v[idx], dtype=self.torch.long)
                for k, v in self.encodings.items()}
        item["labels"] = self.torch.tensor(self.labels[idx], dtype=self.torch.long)
        return item


def freeze_except_last_encoder_and_classifier(model: Any) -> dict[str, int]:
    """Replicate the published partial-fine-tuning design as closely as possible."""
    for p in model.parameters():
        p.requires_grad = False

    bert = getattr(model, "bert", None)
    if bert is None or not hasattr(bert, "encoder") or not hasattr(bert.encoder, "layer"):
        raise TypeError("Expected BertForSequenceClassification-style model with bert.encoder.layer")
    if len(bert.encoder.layer) < 1:
        raise ValueError("BERT encoder has no layers")
    for p in bert.encoder.layer[-1].parameters():
        p.requires_grad = True

    classifier = getattr(model, "classifier", None)
    if classifier is None:
        raise TypeError("Expected model.classifier on BertForSequenceClassification")
    for p in classifier.parameters():
        p.requires_grad = True

    total = sum(p.numel() for p in model.parameters())
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    return {"totalParameters": int(total), "trainableParameters": int(trainable)}


def confusion_metrics(labels: np.ndarray, pred: np.ndarray) -> dict[str, Any]:
    from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score

    cm = confusion_matrix(labels, pred, labels=[0, 1])
    tn, fp, fn, tp = (int(x) for x in cm.ravel())
    return {
        "accuracy": float(accuracy_score(labels, pred)),
        "precision": float(precision_score(labels, pred, zero_division=0)),
        "recall": float(recall_score(labels, pred, zero_division=0)),
        "f1": float(f1_score(labels, pred, zero_division=0)),
        "weightedF1": float(f1_score(labels, pred, average="weighted", zero_division=0)),
        "tn": tn, "fp": fp, "fn": fn, "tp": tp,
    }


def train_one(
    *, task_name: str, label_col: str, backbone: str, tokenizer: Any,
    train: pd.DataFrame, validation: pd.DataFrame, test: pd.DataFrame,
    output_dir: Path, args: argparse.Namespace, torch: Any,
    AutoModelForSequenceClassification: Any, DataCollatorWithPadding: Any,
    Trainer: Any, TrainingArguments: Any,
) -> dict[str, Any]:
    model = AutoModelForSequenceClassification.from_pretrained(
        backbone, num_labels=2, id2label=ID2LABEL, label2id=LABEL2ID, attn_implementation="eager"
    )
    if args.full_finetune:
        param_stats = {
            "totalParameters": int(sum(p.numel() for p in model.parameters())),
            "trainableParameters": int(sum(p.numel() for p in model.parameters())),
        }
        finetune_scope = "all_parameters"
    else:
        param_stats = freeze_except_last_encoder_and_classifier(model)
        finetune_scope = "classifier_plus_final_bert_encoder_layer"

    def encode(frame: pd.DataFrame):
        return tokenizer(
            frame[args.text_col].astype(str).tolist(),
            truncation=True,
            max_length=int(args.max_length),
            padding=False,
        )

    train_ds = EncodedDataset(encode(train), train[label_col].tolist(), torch)
    val_ds = EncodedDataset(encode(validation), validation[label_col].tolist(), torch)
    test_ds = EncodedDataset(encode(test), test[label_col].tolist(), torch)
    collator = DataCollatorWithPadding(tokenizer=tokenizer)

    from sklearn.metrics import f1_score

    def compute_metrics(eval_pred):
        logits, labels = eval_pred
        pred = np.argmax(logits, axis=-1)
        return {
            "f1": float(f1_score(labels, pred, zero_division=0)),
            "weighted_f1": float(f1_score(labels, pred, average="weighted", zero_division=0)),
        }

    task_dir = output_dir / task_name
    trainer_dir = task_dir / "trainer"
    ta_kwargs = dict(
        output_dir=str(trainer_dir),
        num_train_epochs=float(args.epochs),
        learning_rate=float(args.learning_rate),
        weight_decay=float(args.weight_decay),
        warmup_steps=int(args.warmup_steps),
        per_device_train_batch_size=int(args.train_batch_size),
        per_device_eval_batch_size=int(args.eval_batch_size),
        save_strategy="epoch",
        logging_strategy="steps",
        logging_steps=50,
        load_best_model_at_end=True,
        metric_for_best_model="eval_loss",
        greater_is_better=False,
        save_total_limit=2,
        seed=int(args.seed),
        data_seed=int(args.seed),
        report_to=[],
    )
    params = inspect.signature(TrainingArguments.__init__).parameters
    if "eval_strategy" in params:
        ta_kwargs["eval_strategy"] = "epoch"
    else:
        ta_kwargs["evaluation_strategy"] = "epoch"
    training_args = TrainingArguments(**ta_kwargs)

    trainer_kwargs = dict(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        data_collator=collator,
        compute_metrics=compute_metrics,
    )
    tparams = inspect.signature(Trainer.__init__).parameters
    if "processing_class" in tparams:
        trainer_kwargs["processing_class"] = tokenizer
    elif "tokenizer" in tparams:
        trainer_kwargs["tokenizer"] = tokenizer
    trainer = Trainer(**trainer_kwargs)
    trainer.train()

    validation_metrics = trainer.evaluate(val_ds, metric_key_prefix="validation")
    test_output = trainer.predict(test_ds, metric_key_prefix="test")
    test_pred = np.argmax(test_output.predictions, axis=-1)
    test_labels = np.asarray(test_output.label_ids)
    test_metrics = confusion_metrics(test_labels, test_pred)

    final_dir = task_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    trainer.save_model(str(final_dir))
    tokenizer.save_pretrained(str(final_dir))

    detail = pd.DataFrame({
        "rowIndex": test.index.astype(int),
        "label": test_labels.astype(int),
        "prediction": test_pred.astype(int),
    })
    detail.to_csv(task_dir / "test_predictions.csv", index=False)

    return {
        "task": task_name,
        "labelColumn": label_col,
        "finetuneScope": finetune_scope,
        **param_stats,
        "bestCheckpoint": str(trainer.state.best_model_checkpoint),
        "bestMetric": float(trainer.state.best_metric) if trainer.state.best_metric is not None else None,
        "validationMetrics": {k: float(v) for k, v in validation_metrics.items() if isinstance(v, (int, float))},
        "testMetrics": test_metrics,
        "finalModelDir": str(final_dir),
    }


def main() -> int:
    args = parse_args()
    input_csv = args.input_csv.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if not input_csv.exists():
        raise FileNotFoundError(input_csv)
    if output_dir.exists() and any(output_dir.iterdir()):
        if not args.overwrite:
            raise FileExistsError(f"Nonempty output directory: {output_dir}; use --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    random.seed(args.seed)
    np.random.seed(args.seed)

    try:
        import torch
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
            DataCollatorWithPadding,
            Trainer,
            TrainingArguments,
            set_seed,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Training requires torch, transformers, scikit-learn, fugashi, and ipadic"
        ) from exc
    set_seed(args.seed)

    df = pd.read_csv(input_csv, low_memory=False)
    required = {args.text_col, args.positive_label_col, args.negative_label_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Prepared chABSA CSV missing columns: {sorted(missing)}")
    df = df.dropna(subset=[args.text_col]).copy()
    df[args.text_col] = df[args.text_col].astype(str).str.strip()
    df = df.loc[df[args.text_col].ne("")].copy()
    for col in (args.positive_label_col, args.negative_label_col):
        df[col] = pd.to_numeric(df[col], errors="raise").astype(int)
        if not set(df[col].unique()).issubset({0, 1}) or df[col].nunique() < 2:
            raise ValueError(f"{col} must contain both binary classes 0/1")
    if "jointLabel" not in df.columns:
        df["jointLabel"] = (
            df[args.positive_label_col].astype(str) + df[args.negative_label_col].astype(str)
        )

    train, validation, test = make_splits(df, seed=args.seed, mode=args.split_mode)
    split_map = pd.Series("", index=df.index, dtype="string")
    split_map.loc[train.index] = "train"
    split_map.loc[validation.index] = "validation"
    split_map.loc[test.index] = "test"
    splits = df.copy()
    splits["split"] = split_map
    splits.to_csv(output_dir / "sentence_splits.csv", index=False)

    tokenizer = AutoTokenizer.from_pretrained(args.backbone, use_fast=False)
    positive_result = train_one(
        task_name="positive", label_col=args.positive_label_col, backbone=args.backbone,
        tokenizer=tokenizer, train=train, validation=validation, test=test,
        output_dir=output_dir, args=args, torch=torch,
        AutoModelForSequenceClassification=AutoModelForSequenceClassification,
        DataCollatorWithPadding=DataCollatorWithPadding, Trainer=Trainer,
        TrainingArguments=TrainingArguments,
    )
    negative_result = train_one(
        task_name="negative", label_col=args.negative_label_col, backbone=args.backbone,
        tokenizer=tokenizer, train=train, validation=validation, test=test,
        output_dir=output_dir, args=args, torch=torch,
        AutoModelForSequenceClassification=AutoModelForSequenceClassification,
        DataCollatorWithPadding=DataCollatorWithPadding, Trainer=Trainer,
        TrainingArguments=TrainingArguments,
    )

    metadata = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "method": "dual_binary_chabsa_financial_bert",
        "literatureAlignment": "Nakatsuka & Suimon (2024)",
        "inputCsv": str(input_csv),
        "inputSha256": sha256_file(input_csv),
        "backbone": args.backbone,
        "sentenceCount": int(len(df)),
        "splitMode": args.split_mode,
        "splitCounts": {
            "train": int(len(train)), "validation": int(len(validation)), "test": int(len(test))
        },
        "hyperparameters": {
            "maxLength": int(args.max_length),
            "maxEpochs": float(args.epochs),
            "learningRate": float(args.learning_rate),
            "weightDecay": float(args.weight_decay),
            "warmupSteps": int(args.warmup_steps),
            "trainBatchSize": int(args.train_batch_size),
            "evalBatchSize": int(args.eval_batch_size),
            "seed": int(args.seed),
            "fullFinetune": bool(args.full_finetune),
        },
        "positive": positive_result,
        "negative": negative_result,
    }
    (output_dir / "training_metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(metadata, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
