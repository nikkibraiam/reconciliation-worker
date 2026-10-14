from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class TransferStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ProviderStatus(StrEnum):
    SETTLED = "settled"
    FAILED = "failed"
    REVERSED = "reversed"


@dataclass(frozen=True)
class Transfer:
    """Our own record of money we asked the provider to move."""

    id: int
    reference: str
    amount_minor: int
    currency: str
    status: TransferStatus
    created_at: datetime


@dataclass(frozen=True)
class ProviderRecord:
    """One line of the provider's settlement report."""

    provider_id: str
    reference: str | None
    amount_minor: int
    currency: str
    status: ProviderStatus
    settled_at: datetime


class Kind(StrEnum):
    # Both sides agree; nothing to do.
    MATCHED = "matched"
    # Safe to fix automatically: we were waiting, the provider has a final answer.
    SETTLE_PENDING = "settle_pending"
    FAIL_PENDING = "fail_pending"
    # Need a human.
    AMOUNT_MISMATCH = "amount_mismatch"
    STATUS_MISMATCH = "status_mismatch"
    MISSING_AT_PROVIDER = "missing_at_provider"
    UNKNOWN_AT_PROVIDER = "unknown_at_provider"
    DUPLICATE_AT_PROVIDER = "duplicate_at_provider"


ACTIONS = {Kind.SETTLE_PENDING, Kind.FAIL_PENDING}
DISCREPANCIES = set(Kind) - ACTIONS - {Kind.MATCHED}


@dataclass(frozen=True)
class Finding:
    kind: Kind
    # Our reference when known, otherwise the provider id. Discrepancies are
    # deduplicated by (kind, subject) across runs.
    subject: str
    transfer_id: int | None = None
    provider_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def fingerprint(self) -> str:
        return f"{self.kind}:{self.subject}"
