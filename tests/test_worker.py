from datetime import UTC, datetime, timedelta

import pytest

from recon import worker
from recon.models import ProviderRecord, ProviderStatus
from recon.providers import InMemorySource
from recon.worker import Job, LeaseLost, Settings, claim, enqueue, run_job, run_pending

DAY = datetime(2026, 10, 1, tzinfo=UTC)
NEXT_DAY = DAY + timedelta(days=1)
SETTINGS = Settings(grace=timedelta(hours=24))


def add_transfer(conn, reference, amount=10_000, status="pending", created_at=DAY + timedelta(hours=1)):
    conn.execute(
        """
        INSERT INTO transfers (provider, reference, amount_minor, currency, status, created_at)
        VALUES ('bank', %s, %s, 'USD', %s, %s)
        """,
        (reference, amount, status, created_at),
    )


def rec(pid, reference, amount=10_000, status=ProviderStatus.SETTLED, at=DAY + timedelta(hours=3)):
    return ProviderRecord(pid, reference, amount, "USD", status, at)


def scenario(conn):
    """One example of every outcome."""
    add_transfer(conn, "T-SETTLE")  # pending, provider settled -> auto-fix
    add_transfer(conn, "T-FAIL")  # pending, provider failed  -> auto-fix
    add_transfer(conn, "T-OK", status="succeeded")  # matched
    add_transfer(conn, "T-AMOUNT", amount=5_000)  # amount mismatch
    add_transfer(conn, "T-REVERSED", status="succeeded")  # status mismatch
    add_transfer(conn, "T-MISSING")  # no provider record, past grace
    add_transfer(conn, "T-LATE", created_at=DAY - timedelta(days=3))  # old transfer settled in this window
    return InMemorySource(
        [
            rec("P-1", "T-SETTLE"),
            rec("P-2", "T-FAIL", status=ProviderStatus.FAILED),
            rec("P-3", "T-OK"),
            rec("P-4", "T-AMOUNT", amount=4_900),
            rec("P-5", "T-REVERSED", status=ProviderStatus.REVERSED),
            rec("P-6", "T-LATE"),
            rec("P-7", "NOT-OURS"),  # unknown at provider
        ],
        name="bank",
    )


def statuses(conn):
    return {r["reference"]: r["status"] for r in conn.execute("SELECT reference, status FROM transfers")}


def open_discrepancies(conn):
    return sorted(
        (r["kind"], r["subject"]) for r in conn.execute("SELECT kind, subject FROM discrepancies WHERE status = 'open'")
    )


def test_full_run_auto_fixes_and_reports(conn):
    source = scenario(conn)
    enqueue(conn, "bank", DAY, NEXT_DAY)

    assert run_pending(conn, {"bank": source}, SETTINGS) == 1

    assert statuses(conn) == {
        "T-SETTLE": "succeeded",
        "T-FAIL": "failed",
        "T-OK": "succeeded",
        "T-AMOUNT": "pending",
        "T-REVERSED": "succeeded",
        "T-MISSING": "pending",
        "T-LATE": "succeeded",
    }
    assert open_discrepancies(conn) == [
        ("amount_mismatch", "T-AMOUNT"),
        ("missing_at_provider", "T-MISSING"),
        ("status_mismatch", "T-REVERSED"),
        ("unknown_at_provider", "NOT-OURS"),
    ]
    events = conn.execute("SELECT count(*) AS n FROM transfer_events").fetchone()["n"]
    assert events == 3
    job = conn.execute("SELECT status, stats FROM reconciliation_jobs").fetchone()
    assert job["status"] == "done"
    assert job["stats"]["auto_fixed"] == 3


def test_rerunning_a_window_is_idempotent(conn):
    source = scenario(conn)
    enqueue(conn, "bank", DAY, NEXT_DAY)
    run_pending(conn, {"bank": source}, SETTINGS)
    before = (statuses(conn), open_discrepancies(conn))

    enqueue(conn, "bank", DAY, NEXT_DAY)
    run_pending(conn, {"bank": source}, SETTINGS)

    assert (statuses(conn), open_discrepancies(conn)) == before
    counts = conn.execute(
        """
        SELECT (SELECT count(*) FROM transfer_events) AS events,
               (SELECT count(*) FROM provider_records) AS records,
               (SELECT count(*) FROM discrepancies) AS discrepancies
        """
    ).fetchone()
    assert counts == {"events": 3, "records": 7, "discrepancies": 4}


def test_discrepancy_closes_when_provider_corrects_report(conn):
    add_transfer(conn, "T-AMOUNT", amount=5_000)
    wrong = InMemorySource([rec("P-1", "T-AMOUNT", amount=4_900)], name="bank")
    enqueue(conn, "bank", DAY, NEXT_DAY)
    run_pending(conn, {"bank": wrong}, SETTINGS)
    assert open_discrepancies(conn) == [("amount_mismatch", "T-AMOUNT")]

    corrected = InMemorySource([rec("P-1", "T-AMOUNT", amount=5_000)], name="bank")
    enqueue(conn, "bank", DAY, NEXT_DAY)
    run_pending(conn, {"bank": corrected}, SETTINGS)

    assert open_discrepancies(conn) == []
    assert statuses(conn) == {"T-AMOUNT": "succeeded"}
    resolved = conn.execute("SELECT status, resolved_at FROM discrepancies").fetchone()
    assert resolved["status"] == "resolved" and resolved["resolved_at"] is not None


def test_auto_fix_does_not_override_a_concurrent_change(conn, other_conn, monkeypatch):
    add_transfer(conn, "T-1")
    source = InMemorySource([rec("P-1", "T-1", status=ProviderStatus.SETTLED)], name="bank")
    enqueue(conn, "bank", DAY, NEXT_DAY)
    job = claim(conn, ["bank"], SETTINGS)

    # The decision is taken on a pending transfer, but right before it is
    # applied the app cancels the transfer from another connection.
    real_assert_lease = worker._assert_lease

    def app_cancels_first(c, j):
        other_conn.execute("UPDATE transfers SET status = 'failed' WHERE reference = 'T-1'")
        real_assert_lease(c, j)

    monkeypatch.setattr(worker, "_assert_lease", app_cancels_first)
    monkeypatch.setattr(worker, "_ingest", lambda *a: None)
    conn.execute(
        """
        INSERT INTO provider_records (provider, provider_id, reference, amount_minor, currency, status, settled_at)
        VALUES ('bank', 'P-1', 'T-1', 10000, 'USD', 'settled', %s)
        """,
        (DAY + timedelta(hours=3),),
    )

    stats = run_job(conn, job, source, SETTINGS)

    assert stats["auto_fixed"] == 0
    assert statuses(conn) == {"T-1": "failed"}
    assert conn.execute("SELECT count(*) AS n FROM transfer_events").fetchone()["n"] == 0


def test_two_workers_never_claim_the_same_job(conn, other_conn):
    enqueue(conn, "bank", DAY, NEXT_DAY)

    first = claim(conn, ["bank"], SETTINGS)
    second = claim(other_conn, ["bank"], SETTINGS)

    assert first is not None and second is None


def test_expired_lease_is_taken_over_and_old_worker_is_fenced(conn, other_conn):
    add_transfer(conn, "T-1")
    source = InMemorySource([rec("P-1", "T-1")], name="bank")
    enqueue(conn, "bank", DAY, NEXT_DAY)
    stale = claim(conn, ["bank"], SETTINGS)
    conn.execute("UPDATE reconciliation_jobs SET locked_until = now() - interval '1 second'")

    fresh = claim(other_conn, ["bank"], SETTINGS)
    assert fresh is not None and fresh.attempts == stale.attempts + 1

    with pytest.raises(LeaseLost):
        run_job(conn, stale, source, SETTINGS)
    run_job(other_conn, fresh, source, SETTINGS)

    assert statuses(conn) == {"T-1": "succeeded"}
    assert conn.execute("SELECT count(*) AS n FROM transfer_events").fetchone()["n"] == 1


def test_failing_job_is_retried_then_marked_failed(conn):
    class Broken:
        name = "bank"

        def fetch(self, since, until, cursor):
            raise ConnectionError("provider API down")

    enqueue(conn, "bank", DAY, NEXT_DAY)
    settings = Settings(max_attempts=2)

    run_pending(conn, {"bank": Broken()}, settings)

    job = conn.execute("SELECT status, attempts, last_error FROM reconciliation_jobs").fetchone()
    assert job["status"] == "failed"
    assert job["attempts"] == 2
    assert "provider API down" in job["last_error"]


def test_paginated_ingest_is_idempotent(conn):
    records = [rec(f"P-{i}", f"T-{i}") for i in range(7)]
    source = InMemorySource(records, name="bank", page_size=3)
    enqueue(conn, "bank", DAY, NEXT_DAY)
    job = claim(conn, ["bank"], SETTINGS)

    run_job(conn, job, source, SETTINGS)
    conn.execute("UPDATE reconciliation_jobs SET status = 'running'")
    run_job(conn, Job(job.id, job.provider, job.window_start, job.window_end, job.attempts), source, SETTINGS)

    assert conn.execute("SELECT count(*) AS n FROM provider_records").fetchone()["n"] == 7
