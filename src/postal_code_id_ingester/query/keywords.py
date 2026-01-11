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
