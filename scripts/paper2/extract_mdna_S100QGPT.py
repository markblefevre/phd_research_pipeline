#!/usr/bin/env python3
# A manual extraction
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from lxml import html


DOC_ID = "S100QGPT"
EDINET_CODE = "E30126"

zip_path = Path(
    "data/raw/paper2/edinet/E30126/S100QGPT/S100QGPT.zip"
)

ixbrl_member = (
    "XBRL/PublicDoc/"
    "0102010_honbun_jpcrp030000-asr-001_"
    "E30126-000_2022-12-31_01_2023-03-29_ixbrl.htm"
)

output_path = Path(
    "data/interim/paper2/mdna/E30126/S100QGPT.txt"
)

manifest_path = Path(
    "data/interim/paper2/mdna/extraction_manifest.csv"
)

start_heading = (
    "経営者による財政状態、経営成績及び"
    "キャッシュ・フローの状況の分析"
)


# ------------------------------------------------------------
# 1. Read the human-readable iXBRL section from the EDINET ZIP.
# ------------------------------------------------------------
with ZipFile(zip_path) as zf:
    raw = zf.read(ixbrl_member)

root = html.fromstring(raw)

# Find the section-3 MD&A heading.
matches = [
    node
    for node in root.xpath("//h3")
    if start_heading in "".join(node.itertext())
]

if len(matches) != 1:
    raise RuntimeError(
        f"Expected exactly one MD&A heading; found {len(matches)}"
    )

start = matches[0]


# ------------------------------------------------------------
# 2. Collect section 3 only.
#
# The next peer <h3> is section 4, so stop there. This avoids
# accidentally including "重要な契約等" or later sections.
# ------------------------------------------------------------
parts = [start]
node = start.getnext()

while node is not None:
    if (
        isinstance(node.tag, str)
        and node.tag.lower().endswith("h3")
    ):
        break

    parts.append(node)
    node = node.getnext()

if node is None:
    raise RuntimeError("Could not find the next peer <h3> section")


# Convert the selected HTML nodes to normalized plain text.
chunks = []
for part in parts:
    text = part.text_content()
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]
    if lines:
        chunks.append("\n".join(lines))

mdna_text = "\n\n".join(chunks).strip()

if start_heading not in mdna_text:
    raise RuntimeError("Extracted text does not contain MD&A heading")

if "４ 【経営上の重要な契約等】" in mdna_text:
    raise RuntimeError("Extraction accidentally crossed into section 4")

if len(mdna_text) < 1000:
    raise RuntimeError(
        f"Extracted MD&A unexpectedly short: {len(mdna_text)} chars"
    )


# ------------------------------------------------------------
# 3. Write the canonical MD&A text file.
# ------------------------------------------------------------
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(mdna_text + "\n", encoding="utf-8")


# ------------------------------------------------------------
# 4. Correct exactly one manifest row.
#
# "manual_ixbrl" makes the provenance explicit: this document
# was not recovered by the normal XBRL-tag extractor.
# ------------------------------------------------------------
df = pd.read_csv(manifest_path)

mask = df["docID"].eq(DOC_ID)

if mask.sum() != 1:
    raise RuntimeError(
        f"Expected one manifest row for {DOC_ID}; found {mask.sum()}"
    )

df.loc[mask, "status"] = "ok"
df.loc[mask, "method"] = "manual_ixbrl"
df.loc[mask, "matchedLocalName"] = pd.NA
df.loc[mask, "fallbackScore"] = pd.NA
df.loc[mask, "xbrlMember"] = ixbrl_member
df.loc[mask, "textChars"] = len(mdna_text)
df.loc[mask, "error"] = pd.NA
df.loc[mask, "processedAt"] = datetime.now().isoformat(
    timespec="seconds"
)

# Atomic manifest replacement.
tmp = manifest_path.with_suffix(".csv.tmp")
df.to_csv(tmp, index=False)
tmp.replace(manifest_path)

print(f"Extracted {len(mdna_text):,} characters")
print(f"Wrote: {output_path}")
print()
print(
    df.loc[
        mask,
        [
            "docID",
            "edinetCode",
            "status",
            "method",
            "xbrlMember",
            "textChars",
        ],
    ].to_string(index=False)
)
