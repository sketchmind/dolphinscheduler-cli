from __future__ import annotations

import hashlib
import json

from dsctl.errors import ConfirmationRequiredError
from dsctl.output import require_json_object

RISK_CONFIRMATION_PREFIX = "risk"
CONFIRMATION_TOKEN_VERSION = 1


def build_confirmation_token(
    *,
    action: str,
    payload: object,
) -> str:
    """Build a stable confirmation token for one risk-confirmed mutation."""
    normalized_payload = require_json_object(
        payload,
        label="confirmation payload",
    )
    token_payload = {
        "action": action,
        "payload": normalized_payload,
        "version": CONFIRMATION_TOKEN_VERSION,
    }
    encoded = json.dumps(
        token_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:16]
    return f"{RISK_CONFIRMATION_PREFIX}_{digest}"


def require_confirmation(
    *,
    action: str,
    confirmation: str | None,
    payload: object,
    message: str,
    details: object,
) -> None:
    """Require a second explicit confirmation round-trip for one mutation."""
    token = build_confirmation_token(action=action, payload=payload)
    if confirmation == token:
        return

    error_details = {
        **require_json_object(details, label="confirmation details"),
        "confirmation_token": token,
        "confirm_flag": f"--confirm-risk {token}",
    }
    raise ConfirmationRequiredError(message, details=error_details)
