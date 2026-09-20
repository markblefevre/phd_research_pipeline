# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from zipfile import ZipFile, BadZipFile
import html as htmllib
import io
import re

from lxml import etree, html as lxml_html


MDNA_LOCALNAMES = [
    "ManagementAnalysisOfFinancialPositionOperatingResultsAndCashFlowsTextBlock",
    "AnalysisOfFinancialPositionOperatingResultsAndCashFlowsTextBlock",
]

MDNA_ANCHORS = [
    "経営者による財政状態",
    "経営成績",
    "キャッシュ・フロー",
    "財政状態、経営成績及びキャッシュ",
    "財政状態及び経営成績",
]

XHTML_NS = "http://www.w3.org/1999/xhtml"
BLOCK_LOCALNAMES = {"p", "div", "li", "ul", "ol", "table", "tr", "h1", "h2", "h3", "h4", "h5", "h6"}
_TAG_RE = re.compile(r"<[^>]+>")


@dataclass
class MdnaExtractionResult:
    mdna_text: Optional[str]
    status: str
    method: Optional[str] = None
    matched_local_name: Optional[str] = None
    fallback_score: Optional[int] = None
    xbrl_member: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["mdna_chars"] = len(self.mdna_text) if self.mdna_text else 0
        return d


def find_primary_xbrl(zip_path: Path) -> str:
    """Return the most likely Annual Securities Report XBRL member in an EDINET ZIP."""
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(zip_path)

    try:
        with ZipFile(zip_path) as zf:
            members = [
                n for n in zf.namelist()
                if n.startswith("XBRL/PublicDoc/") and n.lower().endswith(".xbrl")
            ]
    except BadZipFile as exc:
        raise ValueError(f"Invalid ZIP: {zip_path}") from exc

    if not members:
        raise FileNotFoundError(f"No PublicDoc .xbrl found in {zip_path}")

    asr_members = [n for n in members if "-asr-" in Path(n).name.lower()]
    if len(asr_members) == 1:
        return asr_members[0]
    if len(asr_members) > 1:
        return max(asr_members, key=lambda n: len(Path(n).name))

    if len(members) == 1:
        return members[0]

    # Conservative fallback: choose the largest PublicDoc XBRL member.
    with ZipFile(zip_path) as zf:
        return max(members, key=lambda n: zf.getinfo(n).file_size)


def node_to_plain_text(node) -> str:
    """Convert an EDINET TextBlock node to normalized plain text."""
    for br in node.findall(f".//{{{XHTML_NS}}}br"):
        br.tail = (br.tail or "") + "\n"

    for el in node.iter():
        try:
            ln = el.tag.split("}")[-1].lower()
        except Exception:
            ln = ""
        if ln in BLOCK_LOCALNAMES:
            el.tail = el.tail or ""
            if not el.tail.endswith("\n\n"):
                el.tail += "\n\n"

    text = "".join(node.itertext())
    text = htmllib.unescape(text)

    if "<" in text and ">" in text:
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(p|div|h[1-6]|li|tr)>", r"</\1>\n\n", text)
        try:
            root = lxml_html.fromstring(f"<div>{text}</div>")
            text = root.text_content()
        except Exception:
            text = _TAG_RE.sub("", text)

    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_mdna_from_xbrl_bytes(xbrl_bytes: bytes) -> MdnaExtractionResult:
    """Extract MD&A from an XBRL document already loaded in memory."""
    parser = etree.XMLParser(recover=True, huge_tree=True)
    try:
        root = etree.parse(io.BytesIO(xbrl_bytes), parser=parser).getroot()
    except Exception as exc:
        return MdnaExtractionResult(
            mdna_text=None,
            status="parse_error",
            error=f"{type(exc).__name__}: {exc}",
        )

    for local_name in MDNA_LOCALNAMES:
        nodes = root.xpath(f".//*[local-name()='{local_name}']")
        if nodes:
            text = node_to_plain_text(nodes[0])
            return MdnaExtractionResult(
                mdna_text=text or None,
                status="ok" if text else "empty_mdna",
                method="tag",
                matched_local_name=local_name,
            )

    textblock_nodes = root.xpath(".//*[contains(local-name(),'TextBlock')]")
    best_node = None
    best_score = 0
    for node in textblock_nodes:
        text = node_to_plain_text(node)
        if not text:
            continue
        score = sum(anchor in text for anchor in MDNA_ANCHORS)
        if score > best_score:
            best_node = node
            best_score = score

    if best_node is not None and best_score >= 3:
        text = node_to_plain_text(best_node)
        return MdnaExtractionResult(
            mdna_text=text or None,
            status="ok" if text else "empty_mdna",
            method="anchor_fallback",
            fallback_score=best_score,
        )

    return MdnaExtractionResult(mdna_text=None, status="mdna_not_found")


def extract_mdna_from_zip(zip_path: Path) -> MdnaExtractionResult:
    """Locate the primary PublicDoc XBRL in an EDINET ZIP and extract MD&A."""
    zip_path = Path(zip_path)
    try:
        member = find_primary_xbrl(zip_path)
        with ZipFile(zip_path) as zf:
            xbrl_bytes = zf.read(member)
    except FileNotFoundError as exc:
        return MdnaExtractionResult(mdna_text=None, status="xbrl_not_found", error=str(exc))
    except BadZipFile as exc:
        return MdnaExtractionResult(mdna_text=None, status="invalid_zip", error=str(exc))
    except Exception as exc:
        return MdnaExtractionResult(mdna_text=None, status="zip_error", error=f"{type(exc).__name__}: {exc}")

    result = extract_mdna_from_xbrl_bytes(xbrl_bytes)
    result.xbrl_member = member
    return result


def write_mdna_text(result: MdnaExtractionResult, output_path: Path) -> None:
    """Write canonical MD&A text if extraction succeeded."""
    if not result.mdna_text:
        raise ValueError(f"No MD&A text to write (status={result.status})")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(result.mdna_text + "\n", encoding="utf-8")
