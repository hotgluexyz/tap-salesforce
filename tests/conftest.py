"""Shared pytest fixtures for tap-salesforce tests."""

import datetime
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock
from io import BytesIO
from openpyxl import Workbook

FIXTURES_DIR = Path(__file__).parent / "fixtures"

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
}


def get_aware_datetime(year=2024, month=1, day=1, hour=0, minute=0, second=0):
    """Helper to create timezone-aware datetime for Singer."""
    return datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)


@pytest.fixture
def sample_config():
    """Sample tap configuration."""
    return SAMPLE_CONFIG.copy()


@pytest.fixture
def mock_response():
    """Factory fixture for creating mock HTTP responses."""
    def _mock_response(json_data):
        resp = MagicMock()
        resp.json.return_value = json_data
        return resp
    return _mock_response


@pytest.fixture
def load_fixture():
    """Load JSON fixture files.
    
    Usage:
        load_fixture("streams/report/basic_response")
        load_fixture("common/empty_responses")
    """
    def _load(name: str):
        if not name.endswith(".json"):
            name = f"{name}.json"
        
        fixture_path = FIXTURES_DIR / name
        
        if not fixture_path.exists():
            raise FileNotFoundError(f"Fixture not found: {fixture_path}")
        
        with open(fixture_path) as f:
            return json.load(f)
    return _load

@pytest.fixture
def load_report_fixture(load_fixture):
    """Load fixtures from streams/report/ directory."""
    def _load(name: str):
        return load_fixture(f"streams/report/{name}")
    return _load


@pytest.fixture
def create_excel_from_fixture():
    """Create Excel file from fixture data.
    
    Usage:
        excel_content = create_excel_from_fixture(load_fixture("streams/report/excel_data/basic_rows"))
    """
    def _create(fixture_data):
        """
        Create an Excel file from fixture data.
        
        Args:
            fixture_data: Dict with keys:
                - header_row: List of header values
                - data_rows: List of lists for data rows
                - include_total: Boolean to include total row (default: True)
        """
        header_row = fixture_data.get("header_row", [])
        data_rows = fixture_data.get("data_rows", [])
        include_total = fixture_data.get("include_total", True)
        
        workbook = Workbook()
        sheet = workbook.active
        
        # Add header row
        sheet.append(header_row)
        
        # Add data rows
        for row in data_rows:
            sheet.append(row)
        
        # Add total row if requested
        if include_total and header_row:
            total_row = [None, "total"] + [None] * (len(header_row) - 2)
            sheet.append(total_row)
        
        # Save to BytesIO
        excel_file = BytesIO()
        workbook.save(excel_file)
        excel_file.seek(0)
        return excel_file.read()
    
    return _create


@pytest.fixture
def mock_sf_client():
    """Mock Salesforce client with common configuration."""
    sf = MagicMock()
    sf._get_standard_headers.return_value = {"Content-Type": "application/json"}
    sf.data_url = "{}/services/data/v52.0/{}"
    sf.instance_url = "https://example.salesforce.com"
    return sf


@pytest.fixture
def catalog_entry_basic(load_fixture):
    """Load basic catalog entry fixture."""
    return load_fixture("streams/report/catalog_entries/basic_report")


@pytest.fixture
def catalog_entry_opportunity(load_fixture):
    """Load opportunity report catalog entry fixture."""
    return load_fixture("streams/report/catalog_entries/opportunity_report")
