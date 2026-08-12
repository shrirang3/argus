"""PII redaction.

Runs inside the SDK, before an event leaves the process — so sensitive text
never crosses the network at all, not even to our own ingestion service. The
ingestion edge redacts again, which covers events arriving from clients we do
not control.

Replacements are typed tokens (`[EMAIL_REDACTED]`) rather than asterisks: the
shape of the data survives, which keeps previews useful for debugging while the
value itself is gone.
"""

from __future__ import annotations

import re
from collections.abc import Callable

# Order matters. Structured identifiers are matched before the looser numeric
# patterns, or a card number gets eaten by the phone rule first.
_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "api_key",
        re.compile(
            r"\b(?:sk-[A-Za-z0-9_-]{16,}"
            r"|gsk_[A-Za-z0-9]{20,}"
            r"|ghp_[A-Za-z0-9]{20,}"
            r"|xox[baprs]-[A-Za-z0-9-]{10,})"
        ),
    ),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")),
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    ("card", re.compile(r"\b(?:\d[ -]?){13,19}\b")),
    ("pan", re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")),
    ("aadhaar", re.compile(r"\b[2-9]\d{3}[ -]?\d{4}[ -]?\d{4}\b")),
    ("phone", re.compile(r"(?:\+\d{1,3}[ -]?)?\b[6-9]\d{9}\b")),
    ("ipv4", re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")),
]

# Patterns whose match must pass an extra test before being treated as a hit.
_VALIDATORS: dict[str, Callable[[str], bool]] = {}


def _luhn(digits: str) -> bool:
    """Luhn checksum — the check digit every real card number satisfies.

    Without it, any 16-digit order number or tracking id gets redacted as a
    card. False positives are not harmless: they destroy the debuggability the
    previews exist for.
    """
    nums = [int(c) for c in digits if c.isdigit()]
    if not 13 <= len(nums) <= 19:
        return False
    checksum = 0
    parity = len(nums) % 2
    for i, n in enumerate(nums):
        if i % 2 == parity:
            n *= 2
            if n > 9:
                n -= 9
        checksum += n
    return checksum % 10 == 0


_VALIDATORS["card"] = lambda m: _luhn(m)


def redact(text: str | None) -> tuple[str | None, dict[str, int]]:
    """Return redacted text and a count of what was replaced.

    The counts are stored alongside the event so redaction can be *proved* to
    have run, rather than assumed. A column of zeroes across the board is a
    signal that the rules are wrong, not that the data was clean.
    """
    if not text:
        return text, {}

    hits: dict[str, int] = {}
    out = text

    for name, pattern in _PATTERNS:
        validator = _VALIDATORS.get(name)

        def _replace(match: re.Match[str], _name: str = name, _v=validator) -> str:
            value = match.group(0)
            if _v is not None and not _v(value):
                return value
            hits[_name] = hits.get(_name, 0) + 1
            return f"[{_name.upper()}_REDACTED]"

        out = pattern.sub(_replace, out)

    return out, hits


def preview(text: str | None, limit: int = 500) -> tuple[str | None, dict[str, int]]:
    """Redact, then truncate to a preview.

    Redaction happens first on purpose: truncating first could slice a card
    number in half and leave the remaining digits unmatched and unredacted.
    """
    redacted, hits = redact(text)
    if redacted is None:
        return None, hits
    if len(redacted) > limit:
        redacted = redacted[:limit] + "…"
    return redacted, hits


def merge_hits(*hit_maps: dict[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for hits in hit_maps:
        for key, count in hits.items():
            merged[key] = merged.get(key, 0) + count
    return merged
