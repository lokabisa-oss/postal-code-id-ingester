import re

STOPWORDS = {
    "desa",
    "kelurahan",
    "gampong",
    "kampung",
    "nagari",
    "dusun",
}

CITY_PREFIXES = {
    "kabupaten",
    "kab",
    "kota",
    "city",
}


def extract_single_word(name: str) -> str:
    """
    Extract a single informative word from a place name.
    Used as a LAST fallback keyword.
    """
    if not name:
        return ""

    cleaned = re.sub(r"[^a-zA-Z\s]", " ", name.lower())
    tokens = [t for t in cleaned.split() if t and t not in STOPWORDS]

    if not tokens:
        return ""

    return max(tokens, key=len)


def extract_prefix_keywords(
    name: str,
    *,
    min_words: int = 2,
    max_words: int = 4,
) -> list[str]:
    """
    Generate progressive prefix keywords.
    'A B C D' -> ['A B', 'A B C', 'A B C D']
    """
    if not name:
        return []

    cleaned = re.sub(r"[^a-zA-Z\s]", " ", name)
    tokens = [t for t in cleaned.split() if t]

    if len(tokens) < min_words:
        return []

    prefixes = []
    for i in range(min(len(tokens), max_words), min_words - 1, -1):
        # build from shorter to longer but caller will dedup/order
        pass

    # build in ascending length (2,3,4)
    prefixes = []
    for i in range(min_words, min(len(tokens), max_words) + 1):
        prefixes.append(" ".join(tokens[:i]))

    return prefixes


def normalize_city_name(name: str) -> str:
    """
    Remove administrative prefixes from city/regency names.
    'Kabupaten Aceh Selatan' -> 'Aceh Selatan'
    'Kota Banda Aceh' -> 'Banda Aceh'
    """
    if not name:
        return ""

    cleaned = re.sub(r"[^a-zA-Z\s]", " ", name.lower())
    parts = [p for p in cleaned.split() if p and p not in CITY_PREFIXES]
    return " ".join(parts).title()
