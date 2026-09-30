"""Utility functions for NovaOS Client SDK."""

import re

try:
    from difflib import SequenceMatcher
except ImportError:
    SequenceMatcher = None  # type: ignore[assignment, misc]


def normalize_capability_name(name: str) -> str:
    """Normalize capability name for consistent matching.

    Normalizes capability names by:
    - Converting to lowercase
    - Replacing underscores and spaces with hyphens
    - Removing extra whitespace
    - Removing special characters (keeping alphanumeric and hyphens)

    Args:
        name: Capability name (e.g., "Web Scraping", "web_scraping", "Web-Scraping")

    Returns:
        Normalized name (e.g., "web-scraping")

    Examples:
        >>> normalize_capability_name("Web Scraping")
        'web-scraping'
        >>> normalize_capability_name("web_scraping")
        'web-scraping'
        >>> normalize_capability_name("API Integration")
        'api-integration'
    """
    if not name:
        return ""

    # Convert to lowercase
    normalized = name.lower().strip()

    # Replace underscores and spaces with hyphens
    normalized = re.sub(r"[_\s]+", "-", normalized)

    # Remove special characters (keep alphanumeric and hyphens)
    normalized = re.sub(r"[^a-z0-9\-]", "", normalized)

    # Remove multiple consecutive hyphens
    normalized = re.sub(r"-+", "-", normalized)

    # Remove leading/trailing hyphens
    normalized = normalized.strip("-")

    return normalized


def _similarity_score(a: str, b: str) -> float:
    """Calculate similarity score between two strings.

    Args:
        a: First string
        b: Second string

    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not a or not b:
        return 0.0

    # Exact match after normalization
    if normalize_capability_name(a) == normalize_capability_name(b):
        return 1.0

    # Use SequenceMatcher for fuzzy matching if available
    if SequenceMatcher is not None:
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()

    # Fallback: simple character overlap
    a_lower = a.lower()
    b_lower = b.lower()
    if not a_lower or not b_lower:
        return 0.0

    # Calculate Jaccard similarity (intersection over union of character sets)
    set_a = set(a_lower)
    set_b = set(b_lower)
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)

    return intersection / union if union > 0 else 0.0


def match_capabilities(
    required: str,
    available: list[str],
    threshold: float = 0.8,
) -> tuple[str, float] | None:
    """Fuzzy match a required capability to available capabilities.

    Args:
        required: Required capability name
        available: List of available capability names
        threshold: Minimum similarity score (0.0-1.0) to consider a match

    Returns:
        Tuple of (matched_capability, similarity_score) if match found above threshold,
        None otherwise

    Examples:
        >>> match_capabilities("web scraping", ["Web Scraping", "Data Analysis"], threshold=0.8)
        ('Web Scraping', 1.0)
        >>> match_capabilities("api", ["API Integration", "Data Analysis"], threshold=0.5)
        ('API Integration', 0.6)
    """
    if not required or not available:
        return None

    # Normalize required capability
    normalized_required = normalize_capability_name(required)

    best_match: tuple[str, float] | None = None
    best_score = 0.0

    for available_cap in available:
        if not available_cap:
            continue

        # Check exact match after normalization
        normalized_available = normalize_capability_name(available_cap)
        if normalized_required == normalized_available:
            return (available_cap, 1.0)

        # Calculate similarity score
        score = _similarity_score(required, available_cap)

        # Also check if required is a substring (for partial matches)
        if (
            normalized_required in normalized_available
            or normalized_available in normalized_required
        ):
            score = max(score, 0.9)

        if score > best_score:
            best_score = score
            best_match = (available_cap, score)

    # Return match if above threshold
    if best_match and best_score >= threshold:
        return best_match

    return None
