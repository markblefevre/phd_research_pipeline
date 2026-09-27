from types import SimpleNamespace

import pytest

from src.mdna_analysis.financial_bert_sentiment import (
    BinaryLabelIds,
    resolve_binary_label_ids,
    split_sentences,
)


def test_split_sentences_japanese_punctuation_and_newlines():
    text = "売上高は増加しました。利益は減少しました！\n来期は改善を見込みます。"
    assert split_sentences(text) == [
        "売上高は増加しました。",
        "利益は減少しました！",
        "来期は改善を見込みます。",
    ]


def test_split_sentences_keeps_nonempty_short_document():
    assert split_sentences("業績") == ["業績"]


def test_resolve_binary_labels_from_checkpoint_metadata():
    cfg = SimpleNamespace(num_labels=2, id2label={0: "absent", 1: "present"})
    assert resolve_binary_label_ids(cfg) == BinaryLabelIds(absent=0, present=1)


def test_resolve_binary_labels_rejects_generic_labels():
    cfg = SimpleNamespace(num_labels=2, id2label={0: "LABEL_0", 1: "LABEL_1"})
    with pytest.raises(ValueError):
        resolve_binary_label_ids(cfg)


def test_resolve_binary_labels_accepts_explicit_ids():
    cfg = SimpleNamespace(num_labels=2, id2label={0: "LABEL_0", 1: "LABEL_1"})
    assert resolve_binary_label_ids(cfg, absent_id=1, present_id=0) == BinaryLabelIds(1, 0)


def test_resolve_binary_labels_requires_binary_model():
    cfg = SimpleNamespace(num_labels=3, id2label={0: "absent", 1: "present", 2: "x"})
    with pytest.raises(ValueError):
        resolve_binary_label_ids(cfg)


def test_binary_label_ids_validate_rejects_collision():
    with pytest.raises(ValueError):
        BinaryLabelIds(0, 0).validate(2)
