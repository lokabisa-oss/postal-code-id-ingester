import re

STOPWORDS = {
    "desa",
    "kelurahan",
    "gampong",
    "kampung",
    "nagari",
    "dusun",
}


def extract_single_word(name: str) -> str:
    """
    Extract a single informative word from a place name.
    Used as a LAST fallback keyword.
    """
    if not name:
        return ""

    # Normalize: lowercase, remove non-letter characters
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", name.lower())
    tokens = [t for t in cleaned.split() if t and t not in STOPWORDS]

    if not tokens:
        return ""

    # Pick the longest token (heuristic: most distinctive)
    return max(tokens, key=len)


def extract_prefix_keywords(
    name: str,
    *,
    min_words: int = 2,
    max_words: int = 4,
) -> list[str]:
    """
    Generate progressive prefix keywords from a place name.

    Example:
    "Matang Glumpang Dua Meunasah Dayah" ->
    [
        "Matang Glumpang",
        "Matang Glumpang Dua",
        "Matang Glumpang Dua Meunasah"
    ]

    Notes:
    - Stopwords are removed only at the beginning.
    - Keeps original word order.
    - Does NOT include single-word prefixes.
    """
    if not name:
        return []

    cleaned = re.sub(r"[^a-zA-Z\s]", " ", name)
    tokens = [t for t in cleaned.split() if t]

    if len(tokens) < min_words:
        return []

    prefixes: list[str] = []
    for i in range(min_words, min(len(tokens), max_words) + 1):
        prefixes.append(" ".join(tokens[:i]))

    return prefixes
