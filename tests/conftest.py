import os
import subprocess

import pytest
from sqlalchemy import create_engine
from testcontainers.postgres import PostgresContainer

# The scoring tests fetch real prices live from Pyth. Default to the Pro
# router (public, no API key) like CI does — the legacy hermes/benchmarks
# endpoint rate-limits (429) under the suite's call volume. `setdefault`
# leaves an explicit `PYTH_BACKEND=hermes` override untouched.
os.environ.setdefault("PYTH_BACKEND", "pro")

postgres = PostgresContainer("postgres:16-alpine")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    os.environ["DB_URL_TEST"] = postgres.get_connection_url()


@pytest.fixture(scope="module", autouse=True)
def apply_migrations(setup):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    alembic_command = ["alembic", "upgrade", "head"]
    subprocess.run(alembic_command, check=True, cwd=project_root)


@pytest.fixture(scope="module", autouse=True)
def db_engine(setup):
    engine = create_engine(os.environ["DB_URL_TEST"])
    yield engine
    engine.dispose()
