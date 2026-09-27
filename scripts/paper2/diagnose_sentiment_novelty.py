#!/usr/bin/env python3

from pathlib import Path
import json

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.stats import pearsonr, spearmanr


NOVELTY = Path(
    "data/interim/paper2/novelty/sudachi_c_num/pair_novelty.csv"
)
LMMD = Path(
    "data/interim/paper2/sentiment/lmmd/lmmd_sentiment.csv"
)
BERT = Path(
    "data/interim/paper2/sentiment/financial_bert/"
    "windows_4080_full/financial_bert_sentiment.csv"
)
OUT = Path(
    "data/interim/paper2/sentiment/financial_bert/"
    "windows_4080_full/diagnostics/novelty_disagreement"
)


def corr(x, y, method="pearson"):
    d = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(d) < 3:
        return np.nan
    if method == "pearson":
        return pearsonr(d["x"], d["y"]).statistic
    return spearmanr(d["x"], d["y"]).statistic


def regression_summary(model):
    return {
        "n": int(model.nobs),
        "r2": float(model.rsquared),
        "adjR2": float(model.rsquared_adj),
        "noveltyCoef": float(model.params["novelty"]),
        "noveltySE": float(model.bse["novelty"]),
        "noveltyT": float(model.tvalues["novelty"]),
        "noveltyP": float(model.pvalues["novelty"]),
    }


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------
    # Load primary Stage 5 novelty pairs
    # ------------------------------------------------------------
    pairs = pd.read_csv(NOVELTY, low_memory=False)

    if len(pairs) != 33046:
        print(f"WARNING: expected 33,046 pairs, found {len(pairs):,}")

    # Current filing fiscal year
    pairs["fiscalYear"] = pd.to_datetime(
        pairs["curr_periodEnd"], errors="coerce"
    ).dt.year

    # Length-change control established during Stage 5 diagnostics
    valid_len = (pairs["prev_textChars"] > 0) & (pairs["curr_textChars"] > 0)

    pairs["absLogLengthChange"] = np.nan
    pairs.loc[valid_len, "absLogLengthChange"] = np.abs(
        np.log(pairs.loc[valid_len, "curr_textChars"])
        - np.log(pairs.loc[valid_len, "prev_textChars"])
    )

    # ------------------------------------------------------------
    # Load current-document sentiment
    # ------------------------------------------------------------
    lmmd = pd.read_csv(LMMD)[
        ["edinetCode", "docID", "lmmdNet"]
    ].rename(columns={"docID": "curr_docID"})

    bert = pd.read_csv(BERT)[
        ["edinetCode", "docID", "bertNet", "bertProbabilityNet"]
    ].rename(columns={"docID": "curr_docID"})

    df = pairs.merge(
        lmmd,
        on=["edinetCode", "curr_docID"],
        how="inner",
        validate="one_to_one",
    )

    df = df.merge(
        bert,
        on=["edinetCode", "curr_docID"],
        how="inner",
        validate="one_to_one",
    )

    print(f"Research-eligible pairs: {len(pairs):,}")
    print(f"Matched sentiment pairs:  {len(df):,}")

    # ------------------------------------------------------------
    # Standardized disagreement
    #
    # Necessary because LMMD and BERT have very different scales.
    # Standardize across the research-eligible analysis sample.
    # ------------------------------------------------------------
    df["lmmdZ"] = (
        (df["lmmdNet"] - df["lmmdNet"].mean())
        / df["lmmdNet"].std(ddof=1)
    )
    df["bertZ"] = (
        (df["bertNet"] - df["bertNet"].mean())
        / df["bertNet"].std(ddof=1)
    )

    df["absZDisagreement"] = np.abs(df["bertZ"] - df["lmmdZ"])

    # Secondary intuitive disagreement measure.
    # Zero is treated as neutral, not positive/negative.
    df["lmmdSign"] = np.sign(df["lmmdNet"])
    df["bertSign"] = np.sign(df["bertNet"])

    nonzero = (df["lmmdSign"] != 0) & (df["bertSign"] != 0)
    df["signDisagreement"] = np.nan
    df.loc[nonzero, "signDisagreement"] = (
        df.loc[nonzero, "lmmdSign"] != df.loc[nonzero, "bertSign"]
    ).astype(float)

    # ------------------------------------------------------------
    # Overall descriptive relationships
    # ------------------------------------------------------------
    overall = {
        "pairCount": int(len(df)),
        "noveltyMean": float(df["novelty"].mean()),
        "noveltyMedian": float(df["novelty"].median()),

        "bertLmmdPearson": corr(df["bertNet"], df["lmmdNet"], "pearson"),
        "bertLmmdSpearman": corr(df["bertNet"], df["lmmdNet"], "spearman"),

        "noveltyAbsZDisagreementPearson":
            corr(df["novelty"], df["absZDisagreement"], "pearson"),

        "noveltyAbsZDisagreementSpearman":
            corr(df["novelty"], df["absZDisagreement"], "spearman"),

        "nonzeroSignCount": int(nonzero.sum()),
        "signDisagreementPct":
            float(df.loc[nonzero, "signDisagreement"].mean() * 100),
    }

    # ------------------------------------------------------------
    # Novelty quintiles and deciles
    # ------------------------------------------------------------
    df["noveltyQuintile"] = pd.qcut(
        df["novelty"], 5, labels=False, duplicates="drop"
    ) + 1

    df["noveltyDecile"] = pd.qcut(
        df["novelty"], 10, labels=False, duplicates="drop"
    ) + 1

    def grouped_summary(group_col):
        rows = []

        for group, g in df.groupby(group_col, observed=True):
            nz = g["signDisagreement"].notna()

            rows.append({
                group_col: int(group),
                "n": int(len(g)),
                "noveltyMean": float(g["novelty"].mean()),
                "noveltyMedian": float(g["novelty"].median()),
                "meanAbsZDisagreement":
                    float(g["absZDisagreement"].mean()),
                "medianAbsZDisagreement":
                    float(g["absZDisagreement"].median()),
                "bertLmmdPearson":
                    corr(g["bertNet"], g["lmmdNet"], "pearson"),
                "bertLmmdSpearman":
                    corr(g["bertNet"], g["lmmdNet"], "spearman"),
                "signDisagreementPct":
                    float(g.loc[nz, "signDisagreement"].mean() * 100)
                    if nz.any() else np.nan,
            })

        return pd.DataFrame(rows)

    quintiles = grouped_summary("noveltyQuintile")
    deciles = grouped_summary("noveltyDecile")

    # ------------------------------------------------------------
    # Near-zero sign-disagreement diagnostic
    #
    # Sign disagreement can be mechanically high when both
    # sentiment measures are close to zero. Test whether the
    # declining sign-disagreement rate across novelty is explained
    # by near-neutral observations.
    # ------------------------------------------------------------

    df["absBertZ"] = np.abs(df["bertZ"])
    df["absLmmdZ"] = np.abs(df["lmmdZ"])

    # Sentiment magnitude by novelty decile
    magnitude_rows = []

    for decile, g in df.groupby("noveltyDecile", observed=True):
        magnitude_rows.append({
            "noveltyDecile": int(decile),
            "n": int(len(g)),
            "noveltyMean": float(g["novelty"].mean()),
            "meanAbsBertZ": float(g["absBertZ"].mean()),
            "medianAbsBertZ": float(g["absBertZ"].median()),
            "meanAbsLmmdZ": float(g["absLmmdZ"].mean()),
            "medianAbsLmmdZ": float(g["absLmmdZ"].median()),
        })

    magnitude_by_decile = pd.DataFrame(magnitude_rows)

    # Sign disagreement after requiring BOTH standardized
    # sentiment measures to be sufficiently far from zero.
    threshold_rows = []

    for threshold in [0.0, 0.10, 0.25, 0.50]:
        eligible = (
            (df["absBertZ"] > threshold)
            & (df["absLmmdZ"] > threshold)
            & df["signDisagreement"].notna()
        )

        d = df.loc[eligible].copy()

        threshold_rows.append({
            "threshold": threshold,
            "n": int(len(d)),
            "shareOfNonzeroSamplePct":
                float(100 * len(d) / nonzero.sum()),
            "signDisagreementPct":
                float(100 * d["signDisagreement"].mean()),
        })

    sign_thresholds = pd.DataFrame(threshold_rows)

    # Same threshold analysis by novelty decile
    threshold_decile_rows = []

    for threshold in [0.0, 0.10, 0.25, 0.50]:
        eligible = (
            (df["absBertZ"] > threshold)
            & (df["absLmmdZ"] > threshold)
            & df["signDisagreement"].notna()
        )

        d = df.loc[eligible].copy()

        for decile, g in d.groupby("noveltyDecile", observed=True):
            threshold_decile_rows.append({
                "threshold": threshold,
                "noveltyDecile": int(decile),
                "n": int(len(g)),
                "signDisagreementPct":
                    float(100 * g["signDisagreement"].mean()),
                "meanAbsBertZ": float(g["absBertZ"].mean()),
                "meanAbsLmmdZ": float(g["absLmmdZ"].mean()),
            })

    sign_thresholds_by_decile = pd.DataFrame(
        threshold_decile_rows
    )

    # ------------------------------------------------------------
    # Pre-specified descriptive regressions
    #
    # HC3 robust SEs.
    # Model 1: novelty only
    # Model 2: + length-change control
    # Model 3: + fiscal-year FE
    # Model 4: Model 3 excluding 2018
    # ------------------------------------------------------------
    reg_df = df[
        [
            "absZDisagreement",
            "novelty",
            "absLogLengthChange",
            "fiscalYear",
        ]
    ].dropna()

    m1 = smf.ols(
        "absZDisagreement ~ novelty",
        data=reg_df,
    ).fit(cov_type="HC3")

    m2 = smf.ols(
        "absZDisagreement ~ novelty + absLogLengthChange",
        data=reg_df,
    ).fit(cov_type="HC3")

    m3 = smf.ols(
        "absZDisagreement ~ novelty + absLogLengthChange + C(fiscalYear)",
        data=reg_df,
    ).fit(cov_type="HC3")

    no2018 = reg_df[reg_df["fiscalYear"] != 2018].copy()

    m4 = smf.ols(
        "absZDisagreement ~ novelty + absLogLengthChange + C(fiscalYear)",
        data=no2018,
    ).fit(cov_type="HC3")

    regressions = {
        "noveltyOnly": regression_summary(m1),
        "noveltyPlusLength": regression_summary(m2),
        "noveltyPlusLengthYearFE": regression_summary(m3),
        "exclude2018PlusLengthYearFE": regression_summary(m4),
    }

    # ------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------
    df.to_csv(OUT / "sentiment_novelty_panel.csv", index=False)
    quintiles.to_csv(OUT / "novelty_quintiles.csv", index=False)
    deciles.to_csv(OUT / "novelty_deciles.csv", index=False)
    magnitude_by_decile.to_csv(
        OUT / "sentiment_magnitude_by_novelty_decile.csv",
        index=False,
    )

    sign_thresholds.to_csv(
        OUT / "sign_disagreement_thresholds.csv",
        index=False,
    )

    sign_thresholds_by_decile.to_csv(
        OUT / "sign_disagreement_thresholds_by_decile.csv",
        index=False,
    )
    summary = {
        "diagnostic": "sentiment_novelty_disagreement",
        "primaryNoveltyVariant": "sudachi_c_num",
        "primaryDisagreementMeasure": "abs(z(bertNet)-z(lmmdNet))",
        "overall": overall,
        "regressions": regressions,
    }

    with open(OUT / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\nOVERALL")
    print(json.dumps(overall, indent=2))

    print("\nNOVELTY QUINTILES")
    print(quintiles.to_string(index=False))

    print("\nREGRESSIONS")
    print(json.dumps(regressions, indent=2))

    print("\nSENTIMENT MAGNITUDE BY NOVELTY DECILE")
    print(magnitude_by_decile.to_string(index=False))

    print("\nSIGN DISAGREEMENT BY NEAR-ZERO THRESHOLD")
    print(sign_thresholds.to_string(index=False))

    print("\nSIGN DISAGREEMENT BY THRESHOLD AND NOVELTY DECILE")
    print(sign_thresholds_by_decile.to_string(index=False))

    print(f"\nOutputs written to:\n{OUT.resolve()}")


if __name__ == "__main__":
    main()