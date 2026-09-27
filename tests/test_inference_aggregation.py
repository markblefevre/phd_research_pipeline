from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import torch

from src.mdna_analysis.financial_bert_sentiment import (
    BinaryLabelIds,
    _score_document_block,
)


class FakeTokenizer:
    unk_token_id = 999
    model_max_length = 512

    def num_special_tokens_to_add(self, pair=False):
        return 2

    def __call__(self, texts, **kwargs):
        # One content token per non-space character.
        if isinstance(texts, str):
            texts = [texts]
        return {"input_ids": [[1 for c in t if not c.isspace()] for t in texts]}

    def prepare_for_model(self, ids, **kwargs):
        vals = [101] + list(ids) + [102]
        return {
            "input_ids": vals,
            "attention_mask": [1] * len(vals),
            "token_type_ids": [0] * len(vals),
        }

    def pad(self, features, padding=True, return_tensors="pt"):
        max_len = max(len(f["input_ids"]) for f in features)
        out = {}
        for key in ("input_ids", "attention_mask", "token_type_ids"):
            fill = 0
            out[key] = torch.tensor(
                [f[key] + [fill] * (max_len - len(f[key])) for f in features],
                dtype=torch.long,
            )
        return out


class ConstantBinaryModel:
    def __init__(self, present=True):
        self.present = present

    def eval(self):
        return self

    def __call__(self, **batch):
        n = batch["input_ids"].shape[0]
        # id0=absent, id1=present
        logits = torch.tensor([[-4.0, 4.0] if self.present else [4.0, -4.0]] * n)
        return SimpleNamespace(logits=logits)


def test_overlong_sentence_is_reaggregated_before_document_count(tmp_path: Path):
    mdna_root = tmp_path / "mdna"
    doc_dir = mdna_root / "E00001"
    doc_dir.mkdir(parents=True)
    # First sentence will exceed max_content_tokens=8 when max_length=10.
    (doc_dir / "D1.txt").write_text("ABCDEFGHIJK。短文。", encoding="utf-8")
    docs = pd.DataFrame([{"edinetCode": "E00001", "docID": "D1"}])

    rows = _score_document_block(
        docs,
        mdna_root=mdna_root,
        tokenizer=FakeTokenizer(),
        positive_model=ConstantBinaryModel(True),
        negative_model=ConstantBinaryModel(False),
        positive_labels=BinaryLabelIds(0, 1),
        negative_labels=BinaryLabelIds(0, 1),
        torch=torch,
        device="cpu",
        min_chars=2,
        max_length=10,
        threshold=0.5,
        inference_batch_size=2,
    )
    r = rows[0]
    assert r["bertSentenceCount"] == 2
    assert r["bertModelPieceCount"] > r["bertSentenceCount"]
    assert r["bertOverlongSentenceCount"] == 1
    assert r["bertPositiveSentenceCount"] == 2
    assert r["bertNegativeSentenceCount"] == 0
    assert r["bertNet"] == 1.0
