import importlib.util
from pathlib import Path
from types import SimpleNamespace

import torch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "paper2" / "train_financial_bert_sentiment.py"
spec = importlib.util.spec_from_file_location("train_finbert", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class DummyModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.embeddings = torch.nn.Linear(2, 2)
        self.bert = SimpleNamespace(
            encoder=SimpleNamespace(
                layer=torch.nn.ModuleList([torch.nn.Linear(2, 2), torch.nn.Linear(2, 2)])
            )
        )
        # Register the fake encoder layers as real child modules too.
        self.encoder_layers = self.bert.encoder.layer
        self.classifier = torch.nn.Linear(2, 2)


def test_freeze_scope_only_last_layer_and_classifier_trainable():
    model = DummyModel()
    stats = mod.freeze_except_last_encoder_and_classifier(model)
    assert not any(p.requires_grad for p in model.embeddings.parameters())
    assert not any(p.requires_grad for p in model.encoder_layers[0].parameters())
    assert all(p.requires_grad for p in model.encoder_layers[-1].parameters())
    assert all(p.requires_grad for p in model.classifier.parameters())
    assert 0 < stats["trainableParameters"] < stats["totalParameters"]
