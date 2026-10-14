"""Pure matching rules: no I/O, so every rule is a plain unit test."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta

from .models import Finding, Kind, ProviderRecord, ProviderStatus, Transfer, TransferStatus


def reconcile(
    transfers: Iterable[Transfer],
    records: Iterable[ProviderRecord],
    *,
    now: datetime,
    grace: timedelta,
) -> list[Finding]:
    """Compare our transfers with the provider's records.

    ``transfers`` must include every transfer referenced by ``records``, even
    if it was created outside the window. Otherwise a late settlement of an
    old transfer would be reported as unknown.
    """
    by_reference: dict[str, list[ProviderRecord]] = defaultdict(list)
    findings: list[Finding] = []

    for record in records:
        if record.reference is None:
            findings.append(_unknown(record))
        else:
            by_reference[record.reference].append(record)

    known = {t.reference: t for t in transfers}

    for reference, matched in by_reference.items():
        if reference not in known:
            findings.extend(_unknown(r) for r in matched)

    for transfer in known.values():
        matched = by_reference.get(transfer.reference, [])
        findings.append(_compare(transfer, matched, now=now, grace=grace))

    return findings


def _compare(transfer: Transfer, records: list[ProviderRecord], *, now: datetime, grace: timedelta) -> Finding:
    base = {"subject": transfer.reference, "transfer_id": transfer.id}

    if not records:
        # Still in flight, or we failed it before it reached the provider.
        if transfer.status is TransferStatus.FAILED or now - transfer.created_at < grace:
            return Finding(Kind.MATCHED, **base)
        return Finding(
            Kind.MISSING_AT_PROVIDER,
            **base,
            details={
                "status": transfer.status,
                "age_hours": round((now - transfer.created_at) / timedelta(hours=1), 1),
            },
        )

    if len(records) > 1:
        return Finding(
            Kind.DUPLICATE_AT_PROVIDER,
            **base,
            details={"provider_ids": sorted(r.provider_id for r in records)},
        )

    record = records[0]
    base["provider_id"] = record.provider_id

    if (record.amount_minor, record.currency) != (transfer.amount_minor, transfer.currency):
        return Finding(
            Kind.AMOUNT_MISMATCH,
            **base,
            details={
                "ours": {"amount_minor": transfer.amount_minor, "currency": transfer.currency},
                "provider": {"amount_minor": record.amount_minor, "currency": record.currency},
            },
        )

    settled = record.status is ProviderStatus.SETTLED

    if transfer.status is TransferStatus.PENDING:
        return Finding(Kind.SETTLE_PENDING if settled else Kind.FAIL_PENDING, **base)

    agrees = settled == (transfer.status is TransferStatus.SUCCEEDED)
    if agrees:
        return Finding(Kind.MATCHED, **base)
    return Finding(
        Kind.STATUS_MISMATCH,
        **base,
        details={"ours": transfer.status, "provider": record.status},
    )


def _unknown(record: ProviderRecord) -> Finding:
    return Finding(
        Kind.UNKNOWN_AT_PROVIDER,
        subject=record.reference or record.provider_id,
        provider_id=record.provider_id,
        details={"amount_minor": record.amount_minor, "currency": record.currency, "status": record.status},
    )
