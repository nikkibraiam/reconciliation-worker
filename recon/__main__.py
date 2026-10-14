"""CLI.

python -m recon run --provider bank --statement examples/statement.csv --from 2026-10-01 --to 2026-10-02
python -m recon report
"""

import argparse
import json
import logging
import os
from datetime import UTC, datetime, time, timedelta

from .providers import CsvStatementSource
from .worker import Settings, connect, enqueue, migrate, run_pending


def _day(value: str) -> datetime:
    return datetime.combine(datetime.fromisoformat(value).date(), time.min, tzinfo=UTC)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="recon", description="Reconcile transfers against provider settlements.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql://localhost/recon"))
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Queue a window and process the queue.")
    run.add_argument("--provider", required=True, help="Provider name, as stored in transfers.provider.")
    run.add_argument("--statement", required=True, help="Provider settlement report (CSV).")
    run.add_argument("--from", dest="since", type=_day, required=True, help="Window start date (UTC, inclusive).")
    run.add_argument("--to", dest="until", type=_day, required=True, help="Window end date (UTC, exclusive).")
    run.add_argument("--grace-hours", type=float, default=24)

    sub.add_parser("report", help="List open discrepancies.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with connect(args.database_url) as conn:
        migrate(conn)
        if args.command == "run":
            source = CsvStatementSource(args.statement, name=args.provider)
            enqueue(conn, args.provider, args.since, args.until)
            run_pending(conn, {args.provider: source}, Settings(grace=timedelta(hours=args.grace_hours)))
            job = conn.execute(
                "SELECT stats FROM reconciliation_jobs WHERE provider = %s AND window_start = %s AND window_end = %s",
                (args.provider, args.since, args.until),
            ).fetchone()
            print(json.dumps(job["stats"], indent=2))
        else:
            rows = conn.execute(
                """
                SELECT kind, subject, details, first_seen_at::date AS since
                FROM discrepancies WHERE status = 'open' ORDER BY kind, subject
                """
            ).fetchall()
            for r in rows:
                print(f"{r['kind']:<22} {r['subject']:<14} since {r['since']}  {json.dumps(r['details'])}")
            print(f"{len(rows)} open discrepancies")


if __name__ == "__main__":
    main()
