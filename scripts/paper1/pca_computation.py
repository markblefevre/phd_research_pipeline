#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run PCA on the two main sentiment variables for Paper 1.

Default project layout:

    ROOT/
      data/curated/paper1/panel/mdna_summary_nikkei225_with_lmmd.csv
      outputs/paper1/{RUN_ID}/tables/

Default PCA variables:

    document_score
    lmmd_net

Example from the repository root:

    python scripts/paper1/pca_computation.py

Example with an explicit run id:

    python scripts/paper1/pca_computation.py --run-id 20260531

Example overriding variables:

    python scripts/paper1/pca_computation.py \
        --var1 document_score \
        --var2 lmmd_net
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------------------
# Project defaults
# ---------------------------------------------------------------------

# This assumes the script lives somewhere like:
#   ROOT/scripts/paper1/pca_computation.py
ROOT = Path(__file__).resolve().parents[2]

DEFAULT_RUN_ID = "1"

DEFAULT_IN_PATH = (
    ROOT / "data/curated/paper1/panel/mdna_summary_nikkei225_with_lmmd.csv"
)

DEFAULT_VAR1 = "document_score"
DEFAULT_VAR2 = "lmmd_net"


def run_pca(input_file, var1, var2, output_file, plot_file=None):
    """
    Run PCA on two variables from a CSV file.

    Parameters
    ----------
    input_file : str or Path
        Input CSV path.
    var1 : str
        First variable name.
    var2 : str
        Second variable name.
    output_file : str or Path
        Output CSV path for PCA scores.
    plot_file : str or Path, optional
        Optional output path for PCA scatter plot.
    """

    input_file = Path(input_file)
    output_file = Path(output_file)

    if plot_file is not None:
        plot_file = Path(plot_file)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    if plot_file is not None:
        plot_file.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------
    # 1. Load data
    # ------------------------------------------------------------
    df = pd.read_csv(input_file)

    if var1 not in df.columns:
        raise ValueError(f"Column not found: {var1}")

    if var2 not in df.columns:
        raise ValueError(f"Column not found: {var2}")

    # Keep identifier columns if present so output is easier to merge later.
    possible_id_cols = [
        "EDINET Code",
        "edinet_code",
        "company_name",
        "submitter_name",
        "fiscal_year",
        "period_end",
        "filing_date",
        "docID",
        "doc_id",
        "FileName",
        "PFileName",
    ]

    id_cols = [c for c in possible_id_cols if c in df.columns]

    data = df[id_cols + [var1, var2]].copy()

    # Drop missing values in the PCA variables only.
    before = len(data)
    data = data.dropna(subset=[var1, var2])
    after = len(data)

    if after < 2:
        raise ValueError("Not enough non-missing observations to run PCA.")

    # ------------------------------------------------------------
    # 2. Standardize variables
    # ------------------------------------------------------------
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data[[var1, var2]])

    data[f"{var1}_z"] = X_scaled[:, 0]
    data[f"{var2}_z"] = X_scaled[:, 1]

    # ------------------------------------------------------------
    # 3. Run PCA
    # ------------------------------------------------------------
    pca = PCA(n_components=2)
    scores = pca.fit_transform(X_scaled)

    # ------------------------------------------------------------
    # 4. Store PCA scores
    # ------------------------------------------------------------
    result = data.copy()
    result["PC1_common_sentiment"] = scores[:, 0]
    result["PC2_method_disagreement"] = scores[:, 1]

    result.to_csv(output_file, index=False)

    # ------------------------------------------------------------
    # 5. Print PCA diagnostics
    # ------------------------------------------------------------
    explained = pca.explained_variance_ratio_

    loadings = pd.DataFrame(
        pca.components_.T,
        index=[var1, var2],
        columns=["PC1_common_sentiment", "PC2_method_disagreement"],
    )

    corr_df = pd.DataFrame(
        {
            var1: X_scaled[:, 0],
            var2: X_scaled[:, 1],
            "PC1_common_sentiment": scores[:, 0],
            "PC2_method_disagreement": scores[:, 1],
        }
    )

    correlations = corr_df.corr().loc[
        [var1, var2],
        ["PC1_common_sentiment", "PC2_method_disagreement"],
    ]

    raw_corr = pd.DataFrame(X_scaled, columns=[var1, var2]).corr().iloc[0, 1]

    print("\nPCA completed successfully.")
    print(f"Project root:       {ROOT}")
    print(f"Input file:         {input_file}")
    print(f"Variables:          {var1}, {var2}")
    print(f"Rows in input file: {before}")
    print(f"Rows used for PCA:  {after}")
    print(f"Rows dropped:       {before - after}")
    print(f"Output file:        {output_file}")

    print("\nRaw correlation between standardized PCA inputs:")
    print(f"corr({var1}, {var2}) = {raw_corr:.4f}")

    print("\nExplained variance ratio:")
    print(f"PC1_common_sentiment:      {explained[0]:.4f}")
    print(f"PC2_method_disagreement:   {explained[1]:.4f}")

    print("\nLoadings:")
    print(loadings.round(4))

    print("\nPCA components:")
    print(
        f"PC1_common_sentiment = "
        f"{loadings.loc[var1, 'PC1_common_sentiment']:.4f} * standardized({var1}) "
        f"+ {loadings.loc[var2, 'PC1_common_sentiment']:.4f} * standardized({var2})"
    )

    print(
        f"PC2_method_disagreement = "
        f"{loadings.loc[var1, 'PC2_method_disagreement']:.4f} * standardized({var1}) "
        f"+ {loadings.loc[var2, 'PC2_method_disagreement']:.4f} * standardized({var2})"
    )

    print("\nCorrelations between standardized variables and PCs:")
    print(correlations.round(4))

    # ------------------------------------------------------------
    # 6. Save diagnostics table
    # ------------------------------------------------------------
    diagnostics_file = output_file.parent / "pca_diagnostics_real.csv"

    diagnostics = pd.DataFrame(
        {
            "metric": [
                "n_input_rows",
                "n_used_rows",
                "n_dropped_rows",
                "input_correlation",
                "pc1_explained_variance_ratio",
                "pc2_explained_variance_ratio",
                f"{var1}_loading_pc1",
                f"{var2}_loading_pc1",
                f"{var1}_loading_pc2",
                f"{var2}_loading_pc2",
            ],
            "value": [
                before,
                after,
                before - after,
                raw_corr,
                explained[0],
                explained[1],
                loadings.loc[var1, "PC1_common_sentiment"],
                loadings.loc[var2, "PC1_common_sentiment"],
                loadings.loc[var1, "PC2_method_disagreement"],
                loadings.loc[var2, "PC2_method_disagreement"],
            ],
        }
    )

    diagnostics.to_csv(diagnostics_file, index=False)
    print(f"\nDiagnostics saved to: {diagnostics_file}")

    # ------------------------------------------------------------
    # 7. Optional plot
    # ------------------------------------------------------------
    if plot_file:
        plt.figure(figsize=(7, 5))
        plt.scatter(
            result["PC1_common_sentiment"],
            result["PC2_method_disagreement"],
            alpha=0.6,
        )
        plt.axhline(0, linewidth=0.8)
        plt.axvline(0, linewidth=0.8)
        plt.xlabel("PC1: common sentiment")
        plt.ylabel("PC2: method disagreement")
        plt.title(f"PCA of {var1} and {var2}")
        plt.tight_layout()
        plt.savefig(plot_file, dpi=300)
        plt.close()

        print(f"Plot saved to:        {plot_file}")

    return result, loadings, correlations


def main():
    parser = argparse.ArgumentParser(
        description="Run paper-aware PCA on document_score and lmmd_net."
    )

    parser.add_argument(
        "--run-id",
        default=DEFAULT_RUN_ID,
        help="Run ID used under outputs/paper1/{RUN_ID}/tables. "
             "Default: current",
    )

    parser.add_argument(
        "--input",
        default=None,
        help="Optional input CSV path. "
             "Default: data/curated/paper1/panel/mdna_summary_nikkei225_with_lmmd.csv",
    )

    parser.add_argument(
        "--var1",
        default=DEFAULT_VAR1,
        help=f"Name of first PCA variable. Default: {DEFAULT_VAR1}",
    )

    parser.add_argument(
        "--var2",
        default=DEFAULT_VAR2,
        help=f"Name of second PCA variable. Default: {DEFAULT_VAR2}",
    )

    parser.add_argument(
        "--output",
        default=None,
        help="Optional output CSV path. "
             "Default: outputs/paper1/{RUN_ID}/tables/pca_scores_real.csv",
    )

    parser.add_argument(
        "--plot",
        default=None,
        help="Optional plot path. "
             "Default: outputs/paper1/{RUN_ID}/tables/pca_plot_real.png",
    )

    args = parser.parse_args()

    out_dir = ROOT / f"outputs/paper1/{args.run_id}/tables"
    out_dir.mkdir(parents=True, exist_ok=True)

    input_path = Path(args.input) if args.input else DEFAULT_IN_PATH
    output_path = Path(args.output) if args.output else out_dir / "pca_scores_real.csv"
    plot_path = Path(args.plot) if args.plot else out_dir / "pca_plot_real.png"

    run_pca(
        input_file=input_path,
        var1=args.var1,
        var2=args.var2,
        output_file=output_path,
        plot_file=plot_path,
    )


if __name__ == "__main__":
    main()
