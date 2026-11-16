"""Who a call is for, read from the execution token and nowhere else.

The task is the token's ``run_id`` claim, which stays the same when the platform re-mints the
token for a long run. The gateway's own ``task_id`` claim is made fresh on every mint and is
never used. The signature is not checked here: the service verifies it; this only decides
which task a call is about.
"""

from __future__ import annotations

import base64
import json

from .errors import CheckpointAuthError, CheckpointIdentityError

__all__ = ["claims_of", "require_task", "run_id_of"]


def claims_of(token: str) -> dict[str, object]:
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (IndexError, ValueError):
        raise CheckpointAuthError("the execution token cannot be read") from None
    if not isinstance(claims, dict):
        raise CheckpointAuthError("the execution token cannot be read")
    return claims


def run_id_of(token: str) -> str:
    run_id = claims_of(token).get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise CheckpointAuthError("the execution token carries no run")
    return run_id


def require_task(token: str, task_id: str) -> str:
    """Return the token's run, or raise if ``task_id`` names anything else."""
    run_id = run_id_of(token)
    if task_id != run_id:
        raise CheckpointIdentityError(
            "the task of a call is the execution token's run; the argument names another task"
        )
    return run_id
