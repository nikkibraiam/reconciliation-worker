"""Job queue and run orchestration.

A job reconciles one provider over one time window:

1. Ingest: page through the provider's settlements for
   [window_start, window_end + settlement_lag) and upsert them. Each page is
   its own transaction, so a crash mid-way only repeats a page.
2. Decide: load our transfers created in the window, plus any transfer the
   provider records point to, and run the pure matching rules.
3. Apply, in a single transaction fenced by the job lease:
   - auto-fix pending transfers with a final provider answer, guarded by
     ``WHERE status = 'pending'`` so a concurrent change by the app wins;
   - upsert discrepancies and close the ones this run no longer finds;
   - mark the job done with its stats.

Running the same window again yields the same state: it is safe to retry
and to re-run on purpose (e.g. after the provider corrects a report).
"""

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .matching import reconcile
from .models import ACTIONS, DISCREPANCIES, Finding, Kind, ProviderRecord, ProviderStatus, Transfer, TransferStatus
from .providers import SettlementSource, iterate

log = logging.getLogger(__name__)

SCHEMA = (Path(__file__).parent / "schema.sql").read_text()


@dataclass(frozen=True)
class Settings:
    # How long a transfer may stay without a provider record before it is
    # reported as missing.
    grace: timedelta = timedelta(hours=24)
    # How long after the window the provider may still settle a transfer.
    settlement_lag: timedelta = timedelta(days=2)
    lease: timedelta = timedelta(minutes=5)
    max_attempts: int = 5


@dataclass(frozen=True)
class Job:
    id: int
    provider: str
    window_start: datetime
    window_end: datetime
    attempts: int


def connect(url: str) -> Connection:
    """Autocommit connection: every ``conn.transaction()`` block is a real
    transaction and standalone statements commit immediately."""
    return psycopg.connect(url, autocommit=True, row_factory=dict_row)


class LeaseLost(Exception):
    """Another worker took the job over; this worker's results are discarded."""


def migrate(conn: Connection) -> None:
    conn.execute(SCHEMA)


def enqueue(conn: Connection, provider: str, window_start: datetime, window_end: datetime) -> int:
    """Queue a window. Re-enqueueing a finished window re-runs it."""
    row = conn.execute(
        """
        INSERT INTO reconciliation_jobs (provider, window_start, window_end)
        VALUES (%s, %s, %s)
        ON CONFLICT (provider, window_start, window_end) DO UPDATE
            SET status = 'queued', attempts = 0, last_error = NULL
            WHERE reconciliation_jobs.status IN ('done', 'failed')
        RETURNING id
        """,
        (provider, window_start, window_end),
    ).fetchone()
    if row is None:  # already queued or running
        row = conn.execute(
            "SELECT id FROM reconciliation_jobs WHERE provider = %s AND window_start = %s AND window_end = %s",
            (provider, window_start, window_end),
        ).fetchone()
    return row["id"]


def claim(conn: Connection, providers: list[str], settings: Settings) -> Job | None:
    """Lease the oldest runnable job. Expired leases (crashed workers) are runnable again."""
    row = conn.execute(
        """
        UPDATE reconciliation_jobs
        SET status = 'running', attempts = attempts + 1, locked_until = now() + %s
        WHERE id = (
            SELECT id FROM reconciliation_jobs
            WHERE provider = ANY(%s)
              AND (status = 'queued' OR (status = 'running' AND locked_until < now()))
            ORDER BY window_start
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, provider, window_start, window_end, attempts
        """,
        (settings.lease, providers),
    ).fetchone()
    return Job(**row) if row else None


def run_pending(conn: Connection, sources: dict[str, SettlementSource], settings: Settings | None = None) -> int:
    """Process jobs until the queue is empty. Returns how many were processed."""
    settings = settings or Settings()
    processed = 0
    while job := claim(conn, list(sources), settings):
        try:
            stats = run_job(conn, job, sources[job.provider], settings)
            log.info("job %s %s [%s, %s) done: %s", job.id, job.provider, job.window_start, job.window_end, stats)
        except LeaseLost:
            log.warning("job %s: lease lost, results discarded", job.id)
        except Exception as exc:
            _fail(conn, job, exc, settings)
        processed += 1
    return processed


def run_job(conn: Connection, job: Job, source: SettlementSource, settings: Settings) -> dict[str, Any]:
    _ingest(conn, job, source, settings)

    now = conn.execute("SELECT now() AS now").fetchone()["now"]
    transfers, records = _load(conn, job, settings)
    findings = reconcile(transfers, records, now=now, grace=settings.grace)

    with conn.transaction():
        _assert_lease(conn, job)
        applied = _apply_actions(conn, job, findings)
        opened = _upsert_discrepancies(conn, job, findings)
        resolved = _resolve_stale(conn, job, findings)
        stats = {
            "transfers": len(transfers),
            "provider_records": len(records),
            "by_kind": dict(Counter(f.kind.value for f in findings)),
            "auto_fixed": applied,
            "discrepancies_open": opened,
            "discrepancies_resolved": resolved,
        }
        conn.execute(
            """
            UPDATE reconciliation_jobs
            SET status = 'done', stats = %s, locked_until = NULL, finished_at = now(), last_error = NULL
            WHERE id = %s
            """,
            (Jsonb(stats), job.id),
        )
    return stats


def _ingest(conn: Connection, job: Job, source: SettlementSource, settings: Settings) -> None:
    for page in iterate(source, job.window_start, job.window_end + settings.settlement_lag):
        with conn.transaction():
            _assert_lease(conn, job)
            for r in page:
                conn.execute(
                    """
                    INSERT INTO provider_records
                        (provider, provider_id, reference, amount_minor, currency, status, settled_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (provider, provider_id) DO UPDATE SET
                        reference = EXCLUDED.reference, amount_minor = EXCLUDED.amount_minor,
                        currency = EXCLUDED.currency, status = EXCLUDED.status,
                        settled_at = EXCLUDED.settled_at, fetched_at = now()
                    """,
                    (job.provider, r.provider_id, r.reference, r.amount_minor, r.currency, r.status, r.settled_at),
                )
            # Keep the lease alive while paging through long reports.
            conn.execute(
                "UPDATE reconciliation_jobs SET locked_until = now() + %s WHERE id = %s", (settings.lease, job.id)
            )


def _load(conn: Connection, job: Job, settings: Settings) -> tuple[list[Transfer], list[ProviderRecord]]:
    records = [
        ProviderRecord(
            provider_id=r["provider_id"],
            reference=r["reference"],
            amount_minor=r["amount_minor"],
            currency=r["currency"],
            status=ProviderStatus(r["status"]),
            settled_at=r["settled_at"],
        )
        for r in conn.execute(
            """
            SELECT * FROM provider_records
            WHERE provider = %s AND settled_at >= %s AND settled_at < %s
            """,
            (job.provider, job.window_start, job.window_end + settings.settlement_lag),
        )
    ]
    references = [r.reference for r in records if r.reference]
    transfers = [
        Transfer(
            id=t["id"],
            reference=t["reference"],
            amount_minor=t["amount_minor"],
            currency=t["currency"],
            status=TransferStatus(t["status"]),
            created_at=t["created_at"],
        )
        for t in conn.execute(
            """
            SELECT * FROM transfers
            WHERE provider = %s
              AND ((created_at >= %s AND created_at < %s) OR reference = ANY(%s))
            """,
            (job.provider, job.window_start, job.window_end, references),
        )
    ]
    return transfers, records


def _apply_actions(conn: Connection, job: Job, findings: list[Finding]) -> int:
    applied = 0
    for f in findings:
        if f.kind not in ACTIONS:
            continue
        to_status = TransferStatus.SUCCEEDED if f.kind is Kind.SETTLE_PENDING else TransferStatus.FAILED
        row = conn.execute(
            """
            UPDATE transfers SET status = %s, provider_id = %s, updated_at = now()
            WHERE id = %s AND status = 'pending'
            RETURNING id
            """,
            (to_status, f.provider_id, f.transfer_id),
        ).fetchone()
        if row is None:
            # Changed by someone else since we read it; the next run re-evaluates.
            continue
        conn.execute(
            "INSERT INTO transfer_events (transfer_id, from_status, to_status, job_id) VALUES (%s, 'pending', %s, %s)",
            (f.transfer_id, to_status, job.id),
        )
        applied += 1
    return applied


def _upsert_discrepancies(conn: Connection, job: Job, findings: list[Finding]) -> int:
    count = 0
    for f in findings:
        if f.kind not in DISCREPANCIES:
            continue
        conn.execute(
            """
            INSERT INTO discrepancies
                (provider, fingerprint, kind, subject, transfer_id, provider_id, details, last_seen_job)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (provider, fingerprint) DO UPDATE SET
                details = EXCLUDED.details, provider_id = EXCLUDED.provider_id,
                last_seen_at = now(), last_seen_job = EXCLUDED.last_seen_job,
                status = 'open', resolved_at = NULL
            """,
            (job.provider, f.fingerprint, f.kind, f.subject, f.transfer_id, f.provider_id, Jsonb(f.details), job.id),
        )
        count += 1
    return count


def _resolve_stale(conn: Connection, job: Job, findings: list[Finding]) -> int:
    """Close open discrepancies about subjects this run examined but no longer flags."""
    examined = sorted({f.subject for f in findings})
    flagged = sorted({f.fingerprint for f in findings if f.kind in DISCREPANCIES})
    cur = conn.execute(
        """
        UPDATE discrepancies SET status = 'resolved', resolved_at = now()
        WHERE provider = %s AND status = 'open'
          AND subject = ANY(%s) AND NOT (fingerprint = ANY(%s))
        """,
        (job.provider, examined, flagged),
    )
    return cur.rowcount


def _assert_lease(conn: Connection, job: Job) -> None:
    # Fencing token: the attempt number we claimed with. A takeover bumps it.
    row = conn.execute(
        "SELECT 1 FROM reconciliation_jobs WHERE id = %s AND status = 'running' AND attempts = %s FOR UPDATE",
        (job.id, job.attempts),
    ).fetchone()
    if row is None:
        raise LeaseLost(job.id)


def _fail(conn: Connection, job: Job, exc: Exception, settings: Settings) -> None:
    final = job.attempts >= settings.max_attempts
    log.exception("job %s attempt %s failed%s", job.id, job.attempts, " permanently" if final else "")
    conn.execute(
        """
        UPDATE reconciliation_jobs SET status = %s, last_error = %s, locked_until = NULL
        WHERE id = %s AND attempts = %s
        """,
        ("failed" if final else "queued", repr(exc), job.id, job.attempts),
    )
