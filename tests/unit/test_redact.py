"""Redaction rules.

The false-positive cases matter as much as the true positives: a rule that
redacts every 16-digit number destroys the debuggability previews exist for.
"""

from argus import redact


def test_email():
    out, hits = redact.redact("mail me at shrirang@example.co.in please")
    assert "shrirang@example.co.in" not in out
    assert "[EMAIL_REDACTED]" in out
    assert hits == {"email": 1}


def test_indian_phone():
    out, hits = redact.redact("call 9876543210")
    assert "9876543210" not in out
    assert hits.get("phone") == 1


def test_valid_card_is_redacted():
    # 4242 4242 4242 4242 is the canonical Luhn-valid test number.
    out, hits = redact.redact("card 4242 4242 4242 4242")
    assert "4242" not in out
    assert hits.get("card") == 1


def test_luhn_rejects_non_card_digits():
    """A 16-digit order number must survive — this is the whole point of Luhn."""
    out, hits = redact.redact("order 1234567812345678")
    assert "1234567812345678" in out
    assert "card" not in hits


def test_api_keys():
    out, hits = redact.redact("key gsk_abcdefghijklmnopqrstuvwxyz012345")
    assert "gsk_" not in out
    assert hits.get("api_key") == 1


def test_pan_and_aadhaar():
    out, hits = redact.redact("PAN ABCDE1234F and Aadhaar 4123 4567 8901")
    assert "ABCDE1234F" not in out
    assert hits.get("pan") == 1
    assert hits.get("aadhaar") == 1


def test_multiple_hits_are_counted():
    _, hits = redact.redact("a@b.com and c@d.com")
    assert hits == {"email": 2}


def test_clean_text_untouched():
    text = "what is my p99 latency in the last hour?"
    out, hits = redact.redact(text)
    assert out == text
    assert hits == {}


def test_preview_redacts_before_truncating():
    """Truncating first could slice an identifier in half and leave it exposed."""
    text = "x" * 495 + " shrirang@example.com " + "y" * 200
    out, hits = redact.preview(text, limit=500)
    assert "shrirang@example.com" not in out
    assert hits.get("email") == 1
    assert len(out) <= 501  # limit plus the ellipsis


def test_none_is_passed_through():
    assert redact.redact(None) == (None, {})
