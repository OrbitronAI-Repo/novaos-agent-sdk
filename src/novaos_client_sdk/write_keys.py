"""The idempotency key of a write, built from the business record and the step.

One scheme for every caller that sends ``X-Idempotency-Key`` on a write: AgentLoop and
``ctx.tools``. The key never depends on the run, the execution or the attempt, so a resume,
a continuation or a redelivery that reaches the same step on the same record sends the same
key.
"""

import hashlib

__all__ = ["idempotency_key"]


def idempotency_key(business_key: str, *, step: int, index: int) -> str:
    """The key for the ``index``-th write of model step ``step`` on one business record.

    ``index`` counts only the step's writes, so reads around a write do not change its key. It
    does not depend on the run, so a rerun that reaches the same step on the same record is
    caught even when it chose different writes.
    """
    if not business_key:
        raise ValueError("business_key is required for a write")
    raw = f"{business_key}\x00{step}\x00{index}".encode()
    return hashlib.sha256(raw).hexdigest()[:32]
