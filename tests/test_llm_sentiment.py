from __future__ import annotations

from src.mdna_analysis.llm_sentiment import (
    DocumentScoringResponse,
    NarrativeUnit,
    UnitScore,
    aggregate_document,
    build_narrative_units,
    clean_mdna_narrative,
)


def test_cleaner_removes_obvious_table_cells_but_keeps_prose():
    text = """
営業収益
31兆3,795億円
15.3%
当連結会計年度の売上高は増加し、営業利益も大幅に増加しました。
資産合計
67兆6,887億円
"""
    narrative, diag = clean_mdna_narrative(text, min_line_chars=12)
    assert "営業利益も大幅に増加しました。" in narrative
    assert "31兆3,795億円" not in narrative
    assert diag.narrative_chars < diag.raw_chars


def test_unit_builder_keeps_complete_sentences():
    text = (
        "売上高は増加しました。"
        "営業利益も増加しました。"
        "一方、原材料価格は上昇しました。"
    )
    units = build_narrative_units(text, target_chars=20, max_chars=35)
    assert units
    assert all(u.text[-1] in "。！？" for u in units)


def test_aggregation_allows_positive_and_negative_to_coexist():
    units = [
        NarrativeUnit(id=1, text="A", chars=100),
        NarrativeUnit(id=2, text="B", chars=300),
    ]
    scores = [
        UnitScore(
            id=1, positive=4, negative=0,
            temporal_focus="realized", risk_related=False
        ),
        UnitScore(
            id=2, positive=0, negative=4,
            temporal_focus="realized", risk_related=True
        ),
    ]
    out = aggregate_document(units, scores)
    assert out["llmPositive"] == 0.25
    assert out["llmNegative"] == 0.75
    assert out["llmNet"] == -0.50
    assert out["llmRiskShare"] == 0.75


def test_structured_schema_accepts_mixed_unit():
    payload = DocumentScoringResponse(
        units=[
            UnitScore(
                id=1,
                positive=2,
                negative=2,
                temporal_focus="mixed",
                risk_related=False,
            )
        ]
    )
    assert payload.units[0].positive == 2
    assert payload.units[0].negative == 2
