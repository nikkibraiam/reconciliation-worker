import os

import pytest

from recon.worker import connect, migrate

DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/recon_test")


@pytest.fixture
def conn():
    with connect(DATABASE_URL) as conn:
        migrate(conn)
        conn.execute(
            "TRUNCATE transfer_events, discrepancies, reconciliation_jobs, provider_records, transfers RESTART IDENTITY"
        )
        yield conn


@pytest.fixture
def other_conn():
    """A second connection, standing in for another worker process."""
    with connect(DATABASE_URL) as conn:
        yield conn
