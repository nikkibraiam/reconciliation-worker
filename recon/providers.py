"""Provider adapters.

The worker only knows the ``SettlementSource`` protocol, so adding a provider
means writing one adapter, never touching the matching or the worker.
"""

import csv
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from .models import ProviderRecord, ProviderStatus


@dataclass(frozen=True)
class Page:
    records: list[ProviderRecord]
    next_cursor: str | None


class SettlementSource(Protocol):
    name: str

    def fetch(self, since: datetime, until: datetime, cursor: str | None) -> Page:
        """Records with since <= settled_at < until, one page at a time."""
        ...


def iterate(source: SettlementSource, since: datetime, until: datetime) -> Iterator[list[ProviderRecord]]:
    cursor = None
    while True:
        page = source.fetch(since, until, cursor)
        yield page.records
        if page.next_cursor is None:
            return
        cursor = page.next_cursor


class InMemorySource:
    """Paginated source over a list; used by tests."""

    def __init__(self, records: Sequence[ProviderRecord], name: str = "memory", page_size: int = 2):
        self.name = name
        self.records = list(records)
        self.page_size = page_size

    def fetch(self, since: datetime, until: datetime, cursor: str | None) -> Page:
        window = sorted(
            (r for r in self.records if since <= r.settled_at < until),
            key=lambda r: (r.settled_at, r.provider_id),
        )
        start = int(cursor or 0)
        end = start + self.page_size
        return Page(window[start:end], str(end) if end < len(window) else None)


class CsvStatementSource(InMemorySource):
    """Settlement report exported as CSV, the most common format in practice.

    Columns: provider_id, reference, amount_minor, currency, status, settled_at (ISO 8601).
    """

    def __init__(self, path: str | Path, name: str = "csv", page_size: int = 500):
        with open(path, newline="", encoding="utf-8") as f:
            records = [
                ProviderRecord(
                    provider_id=row["provider_id"],
                    reference=row["reference"] or None,
                    amount_minor=int(row["amount_minor"]),
                    currency=row["currency"],
                    status=ProviderStatus(row["status"]),
                    settled_at=datetime.fromisoformat(row["settled_at"]),
                )
                for row in csv.DictReader(f)
            ]
        super().__init__(records, name=name, page_size=page_size)
