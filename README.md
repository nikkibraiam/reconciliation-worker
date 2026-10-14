# reconciliation-worker

[![CI](https://github.com/nikkibraiam/reconciliation-worker/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/nikkibraiam/reconciliation-worker/actions/workflows/ci.yml)

Compares **what we think happened** (our `transfers` table) with **what the
provider says happened** (its settlement report). It fixes what is safe to fix
automatically and opens a discrepancy for everything else.

Every payment system needs this. Webhooks get lost, providers settle late,
reverse transfers days later or charge a different amount. Without
reconciliation, those differences only surface when a customer complains or
the books don't close.

Python · PostgreSQL · psycopg 3.

## What it decides

| Our status | Provider says | Result | Action |
|---|---|---|---|
| pending | settled, same amount | `settle_pending` | **auto**: mark succeeded |
| pending | failed / reversed | `fail_pending` | **auto**: mark failed |
| succeeded | settled | `matched` | none |
| failed | failed / no record | `matched` | none |
| any | different amount or currency | `amount_mismatch` | discrepancy |
| succeeded | reversed / failed | `status_mismatch` | discrepancy (money left, we think it arrived) |
| failed | settled | `status_mismatch` | discrepancy (money moved, we think it didn't) |
| pending / succeeded | no record after grace period | `missing_at_provider` | discrepancy |
| no transfer | settled record | `unknown_at_provider` | discrepancy |
| one transfer | several records | `duplicate_at_provider` | discrepancy |

Only transitions out of `pending` are automated. Anything that would
*contradict* a final state, or involves a different amount, goes to a human.

## Example

```bash
docker compose up -d db
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/recon
pip install -e .
python -m recon report                       # creates the schema
psql "$DATABASE_URL" -f examples/seed.sql    # 7 transfers on our side
python -m recon run --provider bank --statement examples/statement.csv --from 2026-10-01 --to 2026-10-02
```

```json
{
  "by_kind": {
    "matched": 1,
    "fail_pending": 1,
    "settle_pending": 2,
    "amount_mismatch": 1,
    "status_mismatch": 1,
    "missing_at_provider": 1,
    "unknown_at_provider": 1
  },
  "transfers": 7,
  "auto_fixed": 3,
  "provider_records": 7,
  "discrepancies_open": 4,
  "discrepancies_resolved": 0
}
```

```
$ python -m recon report
amount_mismatch        TRF-1004       {"ours": {"amount_minor": 500000, ...}, "provider": {"amount_minor": 499000, ...}}
missing_at_provider    TRF-1006       {"status": "pending", "age_hours": 317.2}
status_mismatch        TRF-1005       {"ours": "succeeded", "provider": "reversed"}
unknown_at_provider    BNK-77007      {"status": "settled", "currency": "USD", "amount_minor": 15000}
4 open discrepancies
```

`TRF-0990` was created two days before the window but settled inside it. It
is still matched, because transfers referenced by provider records are always
loaded, whatever their creation date. Running the same command again changes
nothing: `auto_fixed: 0` and the same 4 discrepancies.

## How a run works

```mermaid
flowchart TD
    Q[(reconciliation_jobs)] -->|claim: FOR UPDATE SKIP LOCKED + lease| W[worker]
    W -->|1 - page through report, upsert per page| P[(provider_records)]
    W -->|2 - load window transfers + referenced ones| M[matching.reconcile - pure]
    M -->|3 - one transaction, fenced by lease| A[guarded auto-fixes + transfer_events]
    M --> D[(discrepancies: upsert / auto-close)]
    A --> J[job done + stats]
```

- **Jobs are windows** `(provider, [start, end))`. Provider records are read
  up to `end + settlement_lag`, so late settlements are included.
- **Leasing without session state.** Jobs are claimed with
  `FOR UPDATE SKIP LOCKED` and a `locked_until` lease. A crashed worker's job
  becomes claimable again. There are no advisory locks or session variables,
  so it works behind PgBouncer in transaction mode.
- **Fencing.** The claim increments `attempts`. Every write checks that the
  job still has the attempt number it was claimed with. If another worker
  took over, the old one gets `LeaseLost` and its results are discarded.
- **Guarded auto-fixes.** `UPDATE … WHERE status = 'pending'`: if the
  application changed the transfer after the reconciler read it, the
  application wins and the next run re-evaluates. Every fix is written to
  `transfer_events`.
- **Discrepancy lifecycle.** Discrepancies are keyed by `(kind, subject)`, so
  re-runs update `last_seen_at` instead of creating duplicates. When a later
  run examines the same subject and no longer flags it (for example, the
  provider corrected its report), the discrepancy is closed automatically.
- **Retries.** A failing job goes back to the queue until `max_attempts`, then
  stays `failed` with `last_error`.
- **Provider-agnostic.** Providers implement one method,
  `fetch(since, until, cursor) -> Page`. A CSV statement adapter is included;
  an API adapter is the same interface with HTTP calls.

## Tests

```bash
pip install -e ".[dev]"
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/recon pytest
```

- `tests/test_matching.py`: every rule as a pure unit test (no database).
- `tests/test_worker.py`, against real PostgreSQL:
  - a full scenario with one case of each kind;
  - re-running a window is idempotent;
  - a discrepancy closes when the provider corrects its report;
  - an auto-fix does not override a concurrent change by the app;
  - two workers never claim the same job;
  - an expired lease is taken over and the old worker is fenced;
  - a failing job is retried and then marked failed;
  - paginated ingestion is idempotent.

## What production would add

- An HTTP adapter per provider, with rate limiting and retries.
- A scheduler that enqueues yesterday's window daily and re-runs the last N
  days, because providers do correct reports.
- Alerts on new `status_mismatch` and `amount_mismatch`: these are the ones
  that lose money.
- Ledger postings for auto-fixes, if transfers move balances.

## License

MIT
