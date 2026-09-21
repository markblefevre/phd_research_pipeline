from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd


KEY = ["edinetCode", "docID"]
BAD_STATUSES = {"missing_source", "error"}


def _load_manifest(
    token_root: Path,
    variant: str,
) -> pd.DataFrame:
    """Load and perform basic validation on one Stage 4 manifest."""
    manifest_path = token_root / variant / "manifest.csv"

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Missing tokenization manifest: {manifest_path}"
        )

    df = pd.read_csv(
        manifest_path,
        dtype={
            "edinetCode": "string",
            "docID": "string",
            "sourcePath": "string",
            "tokenPath": "string",
            "variant": "string",
            "status": "string",
        },
    )

    required = {
        "edinetCode",
        "docID",
        "variant",
        "status",
        "tokenCount",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{variant} manifest missing required columns: "
            f"{sorted(missing)}"
        )

    duplicates = int(df.duplicated(KEY).sum())
    if duplicates:
        raise ValueError(
            f"{variant} has {duplicates:,} duplicate "
            "(edinetCode, docID) rows"
        )

    failures = df[df["status"].isin(BAD_STATUSES)]
    if not failures.empty:
        raise ValueError(
            f"{variant} contains {len(failures):,} failed document(s)"
        )

    variant_values = set(
        df["variant"].dropna().astype(str)
    )
    if variant_values != {variant}:
        raise ValueError(
            f"{variant} manifest has unexpected variant values: "
            f"{sorted(variant_values)}"
        )

    if (df["tokenCount"] <= 0).any():
        count = int((df["tokenCount"] <= 0).sum())
        raise ValueError(
            f"{variant} contains {count:,} document(s) "
            "with tokenCount <= 0"
        )

    return df


def _check_physical_files(
    token_root: Path,
    variant: str,
    df: pd.DataFrame,
) -> int:
    """Return the number of missing physical token files."""
    missing = 0

    for row in df.itertuples(index=False):
        path = (
            token_root
            / variant
            / str(row.edinetCode)
            / f"{row.docID}.tokens.txt"
        )

        if not path.exists():
            missing += 1

    return missing


def validate_tokenization_outputs(
    *,
    token_root: str | Path,
    raw_variants: Iterable[str],
    num_variants: Iterable[str] | None = None,
) -> dict:
    """
    Validate Stage 4 tokenization outputs.

    Checks:
      1. manifests exist and contain valid rows
      2. no duplicate documents
      3. identical document universe across variants
      4. no failed or zero-token documents
      5. all physical token files exist
      6. source metadata agrees across raw variants
      7. Sudachi token counts satisfy A >= B >= C
      8. raw and <NUM> token counts match exactly
      9. numeric replacement statistics are internally valid

    Writes:
        <token_root>/qc_summary.json

    Raises:
        ValueError / FileNotFoundError on QC failure.
    """
    token_root = Path(token_root).expanduser().resolve()

    raw_variants = list(raw_variants)
    num_variants = list(num_variants or [])

    all_variants = raw_variants + num_variants

    if not all_variants:
        raise ValueError("No Stage 4 variants supplied for QC")

    manifests = {
        variant: _load_manifest(token_root, variant)
        for variant in all_variants
    }

    summary = {
        "status": "passed",
        "documentCount": None,
        "variants": {},
        "checks": {},
    }

    # ------------------------------------------------------------
    # 1. Same document universe everywhere
    # ------------------------------------------------------------
    reference_variant = all_variants[0]

    reference = (
        manifests[reference_variant]
        .set_index(KEY)
        .sort_index()
    )

    summary["documentCount"] = len(reference)

    for variant, df in manifests.items():
        indexed = df.set_index(KEY).sort_index()

        if not reference.index.equals(indexed.index):
            raise ValueError(
                f"Document universe mismatch: "
                f"{reference_variant} vs {variant}"
            )

    summary["checks"]["sameDocumentUniverse"] = True

    # ------------------------------------------------------------
    # 2. Physical files exist
    # ------------------------------------------------------------
    for variant, df in manifests.items():
        missing_files = _check_physical_files(
            token_root,
            variant,
            df,
        )

        if missing_files:
            raise ValueError(
                f"{variant} is missing "
                f"{missing_files:,} physical token file(s)"
            )

        summary["variants"][variant] = {
            "rows": len(df),
            "totalTokens": int(df["tokenCount"].sum()),
            "meanTokens": float(df["tokenCount"].mean()),
            "missingFiles": missing_files,
        }

    summary["checks"]["allPhysicalFilesExist"] = True

    # ------------------------------------------------------------
    # 3. Source metadata must agree across raw variants
    # ------------------------------------------------------------
    if len(raw_variants) > 1:
        raw_reference = (
            manifests[raw_variants[0]]
            .set_index(KEY)
            .sort_index()
        )

        for variant in raw_variants[1:]:
            candidate = (
                manifests[variant]
                .set_index(KEY)
                .sort_index()
            )

            for column in [
                "sourcePath",
                "textChars",
                "textBytes",
            ]:
                if column not in raw_reference.columns:
                    continue
                if column not in candidate.columns:
                    raise ValueError(
                        f"{variant} missing {column}"
                    )

                if not raw_reference[column].equals(
                    candidate[column]
                ):
                    raise ValueError(
                        f"Raw variant source metadata mismatch "
                        f"in {column}: "
                        f"{raw_variants[0]} vs {variant}"
                    )

    summary["checks"]["rawSourceMetadataMatches"] = True

    # ------------------------------------------------------------
    # 4. Sudachi segmentation relationship:
    #    A >= B >= C
    # ------------------------------------------------------------
    raw_by_mode = {
        variant.split("_")[1]: variant
        for variant in raw_variants
        if variant.startswith("sudachi_")
        and variant.endswith("_raw")
    }

    if {"a", "b", "c"} <= set(raw_by_mode):
        a = (
            manifests[raw_by_mode["a"]]
            .set_index(KEY)
            .sort_index()["tokenCount"]
        )
        b = (
            manifests[raw_by_mode["b"]]
            .set_index(KEY)
            .sort_index()["tokenCount"]
        )
        c = (
            manifests[raw_by_mode["c"]]
            .set_index(KEY)
            .sort_index()["tokenCount"]
        )

        ab_violations = int((a < b).sum())
        bc_violations = int((b < c).sum())

        summary["checks"]["rawA_lt_B_violations"] = (
            ab_violations
        )
        summary["checks"]["rawB_lt_C_violations"] = (
            bc_violations
        )

        if ab_violations or bc_violations:
            raise ValueError(
                "Sudachi raw token-count ordering failed: "
                f"A<B violations={ab_violations:,}, "
                f"B<C violations={bc_violations:,}"
            )

    # ------------------------------------------------------------
    # 5. Raw vs <NUM> must preserve token count exactly
    # ------------------------------------------------------------
    num_by_mode = {
        variant.split("_")[1]: variant
        for variant in num_variants
        if variant.startswith("sudachi_")
        and variant.endswith("_num")
    }

    for mode, raw_variant in raw_by_mode.items():
        num_variant = num_by_mode.get(mode)

        if num_variant is None:
            continue

        raw = (
            manifests[raw_variant]
            .set_index(KEY)
            .sort_index()
        )
        num = (
            manifests[num_variant]
            .set_index(KEY)
            .sort_index()
        )

        mismatch = (
            raw["tokenCount"]
            != num["tokenCount"]
        )

        mismatch_count = int(mismatch.sum())

        summary["checks"][
            f"{mode.upper()}RawNumTokenCountMismatches"
        ] = mismatch_count

        if mismatch_count:
            raise ValueError(
                f"{raw_variant} vs {num_variant}: "
                f"{mismatch_count:,} token-count mismatch(es)"
            )

        if "replacementCount" not in num.columns:
            raise ValueError(
                f"{num_variant} missing replacementCount"
            )

        replacements = int(
            num["replacementCount"]
            .fillna(0)
            .sum()
        )

        total_tokens = int(num["tokenCount"].sum())

        if replacements < 0:
            raise ValueError(
                f"{num_variant} has negative replacementCount"
            )

        if replacements > total_tokens:
            raise ValueError(
                f"{num_variant} has more replacements "
                "than tokens"
            )

        summary["variants"][num_variant][
            "replacementCount"
        ] = replacements

        summary["variants"][num_variant][
            "replacementPct"
        ] = (
            replacements / total_tokens
            if total_tokens
            else 0.0
        )

    # ------------------------------------------------------------
    # 6. A >= B >= C for <NUM> variants too
    # ------------------------------------------------------------
    if {"a", "b", "c"} <= set(num_by_mode):
        a = (
            manifests[num_by_mode["a"]]
            .set_index(KEY)
            .sort_index()["tokenCount"]
        )
        b = (
            manifests[num_by_mode["b"]]
            .set_index(KEY)
            .sort_index()["tokenCount"]
        )
        c = (
            manifests[num_by_mode["c"]]
            .set_index(KEY)
            .sort_index()["tokenCount"]
        )

        ab_violations = int((a < b).sum())
        bc_violations = int((b < c).sum())

        summary["checks"]["numA_lt_B_violations"] = (
            ab_violations
        )
        summary["checks"]["numB_lt_C_violations"] = (
            bc_violations
        )

        if ab_violations or bc_violations:
            raise ValueError(
                "<NUM> token-count ordering failed: "
                f"A<B violations={ab_violations:,}, "
                f"B<C violations={bc_violations:,}"
            )

    # ------------------------------------------------------------
    # Write QC artifact
    # ------------------------------------------------------------
    qc_path = token_root / "qc_summary.json"

    with qc_path.open("w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print()
    print("*** STAGE 4 QC PASSED ***")
    print(
        f"Documents: {summary['documentCount']:,}"
    )

    for variant in all_variants:
        stats = summary["variants"][variant]

        line = (
            f"{variant}: "
            f"tokens={stats['totalTokens']:,}, "
            f"mean={stats['meanTokens']:.1f}"
        )

        if "replacementCount" in stats:
            line += (
                f", replacements="
                f"{stats['replacementCount']:,} "
                f"({100 * stats['replacementPct']:.2f}%)"
            )

        print(line)

    print(f"QC summary: {qc_path}")

    return summary
