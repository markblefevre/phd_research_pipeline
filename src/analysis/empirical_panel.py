from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

PAIR_KEY = ["edinetCode", "prev_docID", "curr_docID"]
DOC_KEY = ["edinetCode", "docID"]


def _norm_string(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _validate_unique(df: pd.DataFrame, key: list[str], name: str) -> None:
    missing = set(key) - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing key columns: {sorted(missing)}")
    dupes = int(df.duplicated(key).sum())
    if dupes:
        raise ValueError(f"{name} contains {dupes:,} duplicate key row(s): {key}")


def _project_sentiment(
    sentiment: pd.DataFrame,
    *,
    prefix: str,
    value_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _validate_unique(sentiment, DOC_KEY, f"{prefix} sentiment")
    missing = set(value_columns) - set(sentiment.columns)
    if missing:
        raise ValueError(
            f"{prefix} sentiment missing required value columns: {sorted(missing)}"
        )

    s = sentiment[DOC_KEY + value_columns].copy()
    s["edinetCode"] = _norm_string(s["edinetCode"])
    s["docID"] = _norm_string(s["docID"])

    curr = s.rename(
        columns={
            "docID": "curr_docID",
            **{c: f"{prefix}_{c}_curr" for c in value_columns},
        }
    )
    prev = s.rename(
        columns={
            "docID": "prev_docID",
            **{c: f"{prefix}_{c}_prev" for c in value_columns},
        }
    )
    return curr, prev


def _project_llm_sentiment(
    sentiment: pd.DataFrame,
    *,
    value_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Project document-level LLM outputs to current/prior filing keys.

    The canonical LLM output is keyed by docID only, unlike LMMD/BERT which
    also carry edinetCode. Validate docID uniqueness explicitly so we can join
    safely without reconstructing issuer IDs from source paths.
    """
    _validate_unique(sentiment, ["docID"], "llm sentiment")
    missing = set(value_columns) - set(sentiment.columns)
    if missing:
        raise ValueError(
            f"llm sentiment missing required value columns: {sorted(missing)}"
        )

    s = sentiment[["docID"] + value_columns].copy()
    s["docID"] = _norm_string(s["docID"])

    curr = s.rename(
        columns={
            "docID": "curr_docID",
            **{c: f"llm_{c}_curr" for c in value_columns},
        }
    )
    prev = s.rename(
        columns={
            "docID": "prev_docID",
            **{c: f"llm_{c}_prev" for c in value_columns},
        }
    )
    return curr, prev


def _derive_fiscal_year(df: pd.DataFrame) -> pd.Series:
    candidates = [
        "curr_periodEnd",
        "curr_period_end",
        "currPeriodEnd",
        "periodEnd_curr",
    ]
    for col in candidates:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce")
            if dt.notna().any():
                return dt.dt.year.astype("Int64")
    raise ValueError(f"Could not derive fiscalYear; checked {candidates}")


def build_empirical_panel(
    analysis_panel: pd.DataFrame,
    lmmd: pd.DataFrame,
    financial_bert: pd.DataFrame,
    llm: pd.DataFrame,
    event_study: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    base = analysis_panel.copy()
    _validate_unique(base, PAIR_KEY, "analysis_panel")
    base["edinetCode"] = _norm_string(base["edinetCode"])
    base["prev_docID"] = _norm_string(base["prev_docID"])
    base["curr_docID"] = _norm_string(base["curr_docID"])
    if "secCode" in base.columns:
        base["secCode"] = _norm_string(base["secCode"])

    # LMMD
    lmmd = lmmd.copy()
    lmmd["edinetCode"] = _norm_string(lmmd["edinetCode"])
    lmmd["docID"] = _norm_string(lmmd["docID"])
    lmmd_curr, lmmd_prev = _project_sentiment(
        lmmd,
        prefix="lmmd",
        value_columns=["lmmdNet"],
    )
    out = base.merge(
        lmmd_curr,
        on=["edinetCode", "curr_docID"],
        how="left",
        validate="one_to_one",
    )
    out = out.merge(
        lmmd_prev,
        on=["edinetCode", "prev_docID"],
        how="left",
        validate="one_to_one",
    )
    out["lmmd_sentiment_curr"] = pd.to_numeric(
        out["lmmd_lmmdNet_curr"], errors="coerce"
    )
    out["lmmd_sentiment_prev"] = pd.to_numeric(
        out["lmmd_lmmdNet_prev"], errors="coerce"
    )
    out["lmmd_sentiment_change"] = (
        out["lmmd_sentiment_curr"] - out["lmmd_sentiment_prev"]
    )

    # Financial BERT
    financial_bert = financial_bert.copy()
    financial_bert["edinetCode"] = _norm_string(financial_bert["edinetCode"])
    financial_bert["docID"] = _norm_string(financial_bert["docID"])
    bert_values = ["bertNet"]
    if "bertProbabilityNet" in financial_bert.columns:
        bert_values.append("bertProbabilityNet")
    bert_curr, bert_prev = _project_sentiment(
        financial_bert,
        prefix="bert",
        value_columns=bert_values,
    )
    out = out.merge(
        bert_curr,
        on=["edinetCode", "curr_docID"],
        how="left",
        validate="one_to_one",
    )
    out = out.merge(
        bert_prev,
        on=["edinetCode", "prev_docID"],
        how="left",
        validate="one_to_one",
    )
    out["financial_bert_sentiment_curr"] = pd.to_numeric(
        out["bert_bertNet_curr"], errors="coerce"
    )
    out["financial_bert_sentiment_prev"] = pd.to_numeric(
        out["bert_bertNet_prev"], errors="coerce"
    )
    out["financial_bert_sentiment_change"] = (
        out["financial_bert_sentiment_curr"]
        - out["financial_bert_sentiment_prev"]
    )
    if "bert_bertProbabilityNet_curr" in out.columns:
        out["bertProbabilityNet_change"] = (
            pd.to_numeric(out["bert_bertProbabilityNet_curr"], errors="coerce")
            - pd.to_numeric(out["bert_bertProbabilityNet_prev"], errors="coerce")
        )

    # LLM / GPT-6 Sol
    llm = llm.copy()
    llm["docID"] = _norm_string(llm["docID"])
    llm_values = ["llmNet"]
    for optional in [
        "llmNetEqualWeight",
        "llmPositive",
        "llmNegative",
        "llmForwardShare",
        "llmRiskShare",
        "llmMedianUnitNet",
        "llmNumUnits",
        "llmNarrativeChars",
        "retentionShare",
    ]:
        if optional in llm.columns:
            llm_values.append(optional)

    llm_curr, llm_prev = _project_llm_sentiment(
        llm,
        value_columns=llm_values,
    )
    out = out.merge(
        llm_curr,
        on="curr_docID",
        how="left",
        validate="one_to_one",
    )
    out = out.merge(
        llm_prev,
        on="prev_docID",
        how="left",
        validate="one_to_one",
    )
    out["llm_sentiment_curr"] = pd.to_numeric(
        out["llm_llmNet_curr"], errors="coerce"
    )
    out["llm_sentiment_prev"] = pd.to_numeric(
        out["llm_llmNet_prev"], errors="coerce"
    )
    out["llm_sentiment_change"] = (
        out["llm_sentiment_curr"] - out["llm_sentiment_prev"]
    )
    if "llm_llmNetEqualWeight_curr" in out.columns:
        out["llmNetEqualWeight_change"] = (
            pd.to_numeric(out["llm_llmNetEqualWeight_curr"], errors="coerce")
            - pd.to_numeric(out["llm_llmNetEqualWeight_prev"], errors="coerce")
        )

    # Stage 7 market reaction
    es = event_study.copy()
    _validate_unique(es, PAIR_KEY, "final_event_study_table")
    es["edinetCode"] = _norm_string(es["edinetCode"])
    es["prev_docID"] = _norm_string(es["prev_docID"])
    es["curr_docID"] = _norm_string(es["curr_docID"])
    market_cols = [c for c in es.columns if c not in PAIR_KEY and c not in out.columns]
    out = out.merge(
        es[PAIR_KEY + market_cols],
        on=PAIR_KEY,
        how="left",
        validate="one_to_one",
        indicator="_stage7_merge",
    )
    missing_stage7 = int(out["_stage7_merge"].ne("both").sum())
    if missing_stage7:
        raise ValueError(f"{missing_stage7:,} Stage 6A pair(s) lack Stage 7F rows")
    out = out.drop(columns="_stage7_merge")

    out["fiscalYear"] = _derive_fiscal_year(out)

    sentiment_cols = [
        "lmmd_sentiment_curr",
        "lmmd_sentiment_prev",
        "lmmd_sentiment_change",
        "financial_bert_sentiment_curr",
        "financial_bert_sentiment_prev",
        "financial_bert_sentiment_change",
        "llm_sentiment_curr",
        "llm_sentiment_prev",
        "llm_sentiment_change",
    ]

    summary = {
        "stage": "8A_empirical_panel",
        "rows": int(len(out)),
        "columns": int(len(out.columns)),
        "pairKey": PAIR_KEY,
        "sentimentDefinitions": {
            "lmmd": {
                "primary": "lmmdNet",
                "current": "lmmd_sentiment_curr",
                "previous": "lmmd_sentiment_prev",
                "change": "current - previous",
            },
            "financial_bert": {
                "primary": "bertNet",
                "current": "financial_bert_sentiment_curr",
                "previous": "financial_bert_sentiment_prev",
                "change": "current - previous",
                "probabilityNetPreserved": bool(
                    "bert_bertProbabilityNet_curr" in out.columns
                ),
            },
            "llm": {
                "primary": "llmNet",
                "current": "llm_sentiment_curr",
                "previous": "llm_sentiment_prev",
                "change": "current - previous",
                "documentKey": "docID",
                "equalWeightPreserved": bool(
                    "llm_llmNetEqualWeight_curr" in out.columns
                ),
            },
        },
        "missingSentimentCounts": {
            c: int(out[c].isna().sum()) for c in sentiment_cols
        },
        "fiscalYearMissing": int(out["fiscalYear"].isna().sum()),
        "stage7RowsMatched": int(len(out)),
    }
    return out.reset_index(drop=True), summary


def write_empirical_panel(
    panel: pd.DataFrame,
    summary: dict,
    *,
    output_csv: str | Path,
    summary_json: str | Path,
) -> None:
    output_csv = Path(output_csv)
    summary_json = Path(summary_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(output_csv, index=False)
    summary_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
