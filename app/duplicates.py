"""Normalisation helpers for duplicate bibliographic records.

Two catalogue entries are a *candidate pair* when their title and creator,
after stripping leading/trailing whitespace and case-folding, form the same
combination – and their identifiers differ.  These helpers are pure so the
same rules are used when generating pairs and when re-validating a manual
judgement against the current published catalogue.
"""

from __future__ import annotations


def normalize_field(value: str | None) -> str:
    """Strip outer whitespace and unify case (Unicode-aware ``casefold``)."""
    if value is None:
        return ""
    return value.strip().casefold()


def grouping_key(title: str | None, creator: str | None) -> tuple[str, str]:
    """The normalised ``(title, creator)`` combination used for grouping."""
    return normalize_field(title), normalize_field(creator)


def canonical_identifiers(left: str, right: str) -> tuple[str, str]:
    """Order two identifiers so a pair has exactly one stored representation."""
    return (left, right) if left <= right else (right, left)
