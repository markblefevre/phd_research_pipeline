from sudachipy import Dictionary, SplitMode
import unicodedata

_SUDACHI_DICT = Dictionary()
_SUDACHI_TOKENIZERS = {}


def _get_sudachi_tokenizer(split_mode: SplitMode):
    """Return a cached Sudachi tokenizer for the requested split mode."""
    key = str(split_mode)

    if key not in _SUDACHI_TOKENIZERS:
        _SUDACHI_TOKENIZERS[key] = _SUDACHI_DICT.create(split_mode)

    return _SUDACHI_TOKENIZERS[key]


def normalize_ja(text: str) -> str:
    return unicodedata.normalize("NFKC", text)


def _slice_by_utf8_bytes(
    text: str,
    start: int,
    max_bytes: int,
) -> tuple[str, int]:
    """
    Return (chunk, next_start) where chunk is text[start:next_start] and
    chunk.encode("utf-8") <= max_bytes, maximizing next_start.

    This preserves the original Paper 1 chunking behavior.
    """
    n = len(text)
    if start >= n:
        return "", start

    if len(text[start:n].encode("utf-8")) <= max_bytes:
        return text[start:n], n

    lo = start + 1
    hi = min(n, start + max(1, max_bytes))
    best = start + 1

    while lo <= hi:
        mid = (lo + hi) // 2
        size = len(text[start:mid].encode("utf-8"))
        if size <= max_bytes:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return text[start:best], best


def _slice_at_natural_boundary(
    text: str,
    start: int,
    max_bytes: int,
) -> tuple[str, int]:
    """
    Return a UTF-8-safe chunk, preferring a natural text boundary.

    Boundary preference:
      1. paragraph break
      2. line break
      3. Japanese sentence punctuation
      4. whitespace
      5. original byte-safe cutoff
    """
    raw_chunk, raw_end = _slice_by_utf8_bytes(text, start, max_bytes)

    if not raw_chunk or raw_end >= len(text):
        return raw_chunk, raw_end

    min_boundary_pos = max(1, len(raw_chunk) // 2)

    pos = raw_chunk.rfind("\n\n", min_boundary_pos)
    if pos != -1:
        end = start + pos + 2
        return text[start:end], end

    pos = raw_chunk.rfind("\n", min_boundary_pos)
    if pos != -1:
        end = start + pos + 1
        return text[start:end], end

    best_pos = max(
        raw_chunk.rfind("。", min_boundary_pos),
        raw_chunk.rfind("！", min_boundary_pos),
        raw_chunk.rfind("？", min_boundary_pos),
    )
    if best_pos != -1:
        end = start + best_pos + 1
        return text[start:end], end

    best_pos = max(
        raw_chunk.rfind(" ", min_boundary_pos),
        raw_chunk.rfind("\t", min_boundary_pos),
        raw_chunk.rfind("\u3000", min_boundary_pos),
    )
    if best_pos != -1:
        end = start + best_pos + 1
        return text[start:end], end

    return raw_chunk, raw_end


def tokenize_ja_safe(
    text: str,
    split_mode: SplitMode = SplitMode.C,
    max_bytes: int = 48000,  # stay below Sudachi hard limit (~49149)
    normalize: bool = True,
    prefer_natural_boundaries: bool = False,
):
    """
    Safely tokenize long Japanese text using Sudachi.

    Paper 1 compatibility is preserved by default:
    prefer_natural_boundaries=False uses the original byte-maximizing chunking.

    Paper 2 can opt in with prefer_natural_boundaries=True.
    """
    if normalize:
        text = normalize_ja(text)

    tokens = []
    start = 0
    n = len(text)

    slicer = (
        _slice_at_natural_boundary
        if prefer_natural_boundaries
        else _slice_by_utf8_bytes
    )

    while start < n:
        chunk, next_start = slicer(text, start, max_bytes)
        if not chunk:
            break

        if len(chunk.encode("utf-8")) > max_bytes:
            raise ValueError(
                "Internal chunking error: UTF-8 chunk exceeds max_bytes"
            )

        sudachi_tokenizer = _get_sudachi_tokenizer(split_mode)
        tokens.extend(
            [m.surface() for m in sudachi_tokenizer.tokenize(chunk)]
        )
        start = next_start

    return tokens
