"""Artist name normalization utilities."""

import re
import unicodedata


def normalize_artist_name(name: str) -> str:
    """Normalize an artist name for consistent matching.

    Steps:
        1. Lowercase
        2. Unicode NFKD decomposition (remove diacritics)
        3. Strip leading "the "
        4. Remove special characters: & ! ' - . ,
        5. Collapse multiple spaces into one
        6. Strip whitespace

    Args:
        name: Raw artist name.

    Returns:
        Normalized string.
    """
    # Lowercase
    result = name.lower().strip()

    # Decompose unicode and remove combining characters (accents)
    result = unicodedata.normalize("NFKD", result)
    result = "".join(ch for ch in result if not unicodedata.combining(ch))

    # Remove special characters
    result = re.sub(r"[&!'\-.,]", "", result)

    # Collapse whitespace
    result = re.sub(r"\s+", " ", result).strip()

    # Remove leading "the "
    result = re.sub(r"^the\s+", "", result)

    return result
