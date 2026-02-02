import json
from pathlib import Path
import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def host_config_response():
    """Load Host PMS config response from fixture."""
    with open(FIXTURES_DIR / "host_pms_api" / "config_response.json") as f:
        return json.load(f)


@pytest.fixture
def host_reservation_response():
    """Load Host PMS reservation response from fixture."""
    with open(FIXTURES_DIR / "host_pms_api" / "reservation_response.json") as f:
        return json.load(f)


@pytest.fixture
def host_inventory_response():
    """Load Host PMS inventory response from fixture."""
    with open(FIXTURES_DIR / "host_pms_api" / "inventory_response.json") as f:
        return json.load(f)


@pytest.fixture
def host_revenue_response():
    """Load Host PMS revenue response from fixture."""
    with open(FIXTURES_DIR / "host_pms_api" / "revenue_response.json") as f:
        return json.load(f)


@pytest.fixture
def climber_reservation_example():
    """Load expected Climber reservation format."""
    with open(FIXTURES_DIR / "transformed" / "reservation_climber_format.json") as f:
        return json.load(f)


@pytest.fixture
def climber_inventory_example():
    """Load expected Climber inventory format."""
    with open(FIXTURES_DIR / "transformed" / "inventory_climber_format.json") as f:
        return json.load(f)


@pytest.fixture
def cancelled_reservation():
    """Load cancelled reservation test data."""
    with open(FIXTURES_DIR / "edge_cases" / "cancelled_reservation.json") as f:
        return json.load(f)
