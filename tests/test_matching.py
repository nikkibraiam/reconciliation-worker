from datetime import UTC, datetime, timedelta

import pytest

from recon.matching import reconcile
from recon.models import Kind, ProviderRecord, ProviderStatus, Transfer, TransferStatus

NOW = datetime(2026, 10, 3, tzinfo=UTC)
GRACE = timedelta(hours=24)
OLD = NOW - timedelta(days=2)


def transfer(ref="T-1", amount=10_000, status=TransferStatus.PENDING, created_at=OLD, currency="USD"):
    return Transfer(id=1, reference=ref, amount_minor=amount, currency=currency, status=status, created_at=created_at)


def record(ref="T-1", amount=10_000, status=ProviderStatus.SETTLED, pid="P-1", currency="USD"):
    return ProviderRecord(
        provider_id=pid, reference=ref, amount_minor=amount, currency=currency, status=status, settled_at=OLD
    )


def kinds(transfers, records):
    return sorted(f.kind for f in reconcile(transfers, records, now=NOW, grace=GRACE))


@pytest.mark.parametrize(
    ("ours", "theirs", "expected"),
    [
        (TransferStatus.PENDING, ProviderStatus.SETTLED, Kind.SETTLE_PENDING),
        (TransferStatus.PENDING, ProviderStatus.FAILED, Kind.FAIL_PENDING),
        (TransferStatus.PENDING, ProviderStatus.REVERSED, Kind.FAIL_PENDING),
        (TransferStatus.SUCCEEDED, ProviderStatus.SETTLED, Kind.MATCHED),
        (TransferStatus.FAILED, ProviderStatus.FAILED, Kind.MATCHED),
        (TransferStatus.SUCCEEDED, ProviderStatus.REVERSED, Kind.STATUS_MISMATCH),
        (TransferStatus.SUCCEEDED, ProviderStatus.FAILED, Kind.STATUS_MISMATCH),
        (TransferStatus.FAILED, ProviderStatus.SETTLED, Kind.STATUS_MISMATCH),
    ],
)
def test_status_combinations(ours, theirs, expected):
    assert kinds([transfer(status=ours)], [record(status=theirs)]) == [expected]


def test_amount_mismatch_is_never_auto_fixed():
    findings = reconcile([transfer()], [record(amount=9_999)], now=NOW, grace=GRACE)

    assert [f.kind for f in findings] == [Kind.AMOUNT_MISMATCH]
    assert findings[0].details["provider"]["amount_minor"] == 9_999


def test_currency_mismatch_is_an_amount_mismatch():
    assert kinds([transfer()], [record(currency="EUR")]) == [Kind.AMOUNT_MISMATCH]


def test_missing_at_provider_only_after_grace_period():
    fresh = transfer(created_at=NOW - timedelta(hours=2))
    stale = transfer(ref="T-2", created_at=NOW - timedelta(hours=30))

    findings = {f.subject: f.kind for f in reconcile([fresh, stale], [], now=NOW, grace=GRACE)}

    assert findings == {"T-1": Kind.MATCHED, "T-2": Kind.MISSING_AT_PROVIDER}


def test_failed_transfer_without_provider_record_is_fine():
    assert kinds([transfer(status=TransferStatus.FAILED)], []) == [Kind.MATCHED]


def test_unknown_at_provider_with_and_without_reference():
    findings = reconcile([], [record(ref="X-9", pid="P-9"), record(ref=None, pid="P-10")], now=NOW, grace=GRACE)

    assert sorted((f.kind, f.subject) for f in findings) == [
        (Kind.UNKNOWN_AT_PROVIDER, "P-10"),
        (Kind.UNKNOWN_AT_PROVIDER, "X-9"),
    ]


def test_duplicate_at_provider():
    findings = reconcile([transfer()], [record(pid="P-1"), record(pid="P-2")], now=NOW, grace=GRACE)

    assert [f.kind for f in findings] == [Kind.DUPLICATE_AT_PROVIDER]
    assert findings[0].details["provider_ids"] == ["P-1", "P-2"]


def test_fingerprint_is_stable_across_runs():
    first = reconcile([transfer()], [record(amount=1)], now=NOW, grace=GRACE)[0]
    second = reconcile([transfer()], [record(amount=2)], now=NOW + timedelta(days=1), grace=GRACE)[0]

    assert first.fingerprint == second.fingerprint == "amount_mismatch:T-1"
