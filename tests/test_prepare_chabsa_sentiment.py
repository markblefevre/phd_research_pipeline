import json
from pathlib import Path

from scripts.paper2.prepare_chabsa_sentiment import convert


def test_chabsa_conversion_constructs_independent_binary_labels(tmp_path: Path):
    obj = {
        "header": {"document_id": "E00001", "edi_id": "E00001", "document_name": "Test"},
        "sentences": [
            {"sentence_id": 0, "sentence": "positive", "opinions": [
                {"polarity": "positive", "target": "x", "category": "company#profit"}
            ]},
            {"sentence_id": 1, "sentence": "negative", "opinions": [
                {"polarity": "negative", "target": "x", "category": "company#profit"}
            ]},
            {"sentence_id": 2, "sentence": "both", "opinions": [
                {"polarity": "positive", "target": "x", "category": "company#profit"},
                {"polarity": "negative", "target": "y", "category": "company#sales"},
            ]},
            {"sentence_id": 3, "sentence": "none", "opinions": []},
        ],
    }
    (tmp_path / "E00001_ann.json").write_text(json.dumps(obj), encoding="utf-8")
    df, summary = convert(tmp_path, "*_ann.json")
    labels = list(zip(df["positiveLabel"], df["negativeLabel"]))
    assert labels == [(1, 0), (0, 1), (1, 1), (0, 0)]
    assert summary["sentenceCount"] == 4
    assert summary["documentCount"] == 1
