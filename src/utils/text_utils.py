from sudachipy import dictionary, tokenizer
import unicodedata

_TOK = dictionary.Dictionary().create()

def normalize_ja(text: str) -> str:
    return unicodedata.normalize("NFKC", text)

def _slice_by_utf8_bytes(text: str, start: int, max_bytes: int) -> tuple[str, int]:
    """
    Return (chunk, next_start) where chunk is text[start:next_start] and
    chunk.encode('utf-8') <= max_bytes, maximizing next_start.
    """
    n = len(text)
    if start >= n:
        return "", start

    # Fast path: try a rough end, then shrink via binary search
    lo = start + 1
    hi = n

    # If even the rest fits, take it
    if len(text[start:hi].encode("utf-8")) <= max_bytes:
        return text[start:hi], hi

    # Binary search for the largest end that fits
    hi = min(n, start + max(1, max_bytes))  # upper bound guess
    # Ensure hi is actually too large; if not, expand until too large or end
    while hi < n and len(text[start:hi].encode("utf-8")) <= max_bytes:
        hi = min(n, hi * 2)  # expand exponentially
    # Now binary search between start..hi
    left, right = start + 1, hi
    best = start + 1
    while left <= right:
        mid = (left + right) // 2
        b = len(text[start:mid].encode("utf-8"))
        if b <= max_bytes:
            best = mid
            left = mid + 1
        else:
            right = mid - 1

    return text[start:best], best

def tokenize_ja_safe(
    text: str,
    split_mode: tokenizer.Tokenizer.SplitMode = tokenizer.Tokenizer.SplitMode.C,
    max_bytes: int = 48000,  # stay below Sudachi hard limit (~49149)
    normalize: bool = True,
):
    """
    Safely tokenize long Japanese text using Sudachi, chunking by UTF-8 byte size.
    """
    if normalize:
        text = normalize_ja(text)

    tokens = []
    start = 0
    n = len(text)

    while start < n:
        chunk, next_start = _slice_by_utf8_bytes(text, start, max_bytes)
        if not chunk:
            break
        tokens.extend([m.surface() for m in _TOK.tokenize(chunk, split_mode)])
        start = next_start

    return tokens
