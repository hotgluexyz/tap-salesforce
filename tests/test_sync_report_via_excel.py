"""Tests for sync_report_via_excel method."""

import datetime
import pytest
from io import BytesIO
from unittest.mock import MagicMock, patch
from openpyxl import Workbook

from tap_salesforce.sync import sync_report_via_excel
from tests.conftest import get_aware_datetime


@pytest.fixture
def create_excel_file():
    """Factory fixture to create Excel files with custom data."""
    def _create(header_row, data_rows, include_total=True):
        """
        Create an Excel file in memory.
        
        Args:
            header_row: Tuple of header values (e.g., (None, "Name", "Amount", "Status"))
            data_rows: List of tuples for data rows
            include_total: Whether to include a "total" row at the end
        """
        workbook = Workbook()
        sheet = workbook.active
        
        # Add header row
        sheet.append(header_row)
        
        # Add data rows
        for row in data_rows:
            sheet.append(row)
        
        # Add total row if requested
        if include_total:
            total_row = [None, "total", None, None][:len(header_row)]
            sheet.append(total_row)
        
        # Save to BytesIO
        excel_file = BytesIO()
        workbook.save(excel_file)
        excel_file.seek(0)
        return excel_file.read()
    
    return _create


@pytest.fixture
def sample_catalog_entry(catalog_entry_basic):
    """Sample catalog entry for a report (using fixture)."""
    return catalog_entry_basic


class TestSyncReportViaExcel:
    """Test cases for sync_report_via_excel method."""
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_basic_report_sync(self, mock_write_message, mock_sf_client, sample_catalog_entry, 
                               load_fixture, create_excel_from_fixture):
        """Test syncing a basic report with valid data using fixtures."""
        # Arrange - Load fixture data
        excel_data = load_fixture("streams/report/excel_data/basic_rows")
        excel_content = create_excel_from_fixture(excel_data)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        stream = "Report_TestReport"
        stream_alias = None
        stream_version = 12345
        start_time = get_aware_datetime()
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            stream,
            stream_alias,
            stream_version,
            start_time
        )
        
        # Assert
        # Verify API call was made with correct parameters
        mock_sf_client._make_request.assert_called_once()
        call_args = mock_sf_client._make_request.call_args
        assert call_args[0][0] == 'POST'
        assert 'analytics/reports/00O1234567890ABC' in call_args[0][1]
        assert call_args[1]['headers']['Accept'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        assert call_args[1]['params']['xf'] == 'xlsx'
        assert call_args[1]['params']['includeDetails'] is True
        
        # Verify 3 records were written
        assert mock_write_message.call_count == 3
        
        # Verify first record
        first_call = mock_write_message.call_args_list[0][0][0]
        assert first_call.stream == stream
        # Note: None headers are filtered out by the code (line 624 in sync.py)
        assert first_call.record == {
            "Name": "Deal 1",
            "Amount": 1000.50,
            "Status": "Open",
            "Date": "2024-01-15"
        }
        assert first_call.version == stream_version
        assert first_call.time_extracted == start_time
        
        # Verify second record
        second_call = mock_write_message.call_args_list[1][0][0]
        assert second_call.record["Name"] == "Deal 2"
        assert second_call.record["Amount"] == 2500.75
        
        # Verify third record
        third_call = mock_write_message.call_args_list[2][0][0]
        assert third_call.record["Name"] == "Deal 3"
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_with_stream_alias(self, mock_write_message, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test syncing uses stream_alias when provided."""
        # Arrange
        header_row = (None, "Name", "Amount")
        data_rows = [(None, "Deal 1", 1000)]
        excel_content = create_excel_file(header_row, data_rows)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        stream = "Report_TestReport"
        stream_alias = "CustomAlias"
        stream_version = 12345
        start_time = get_aware_datetime()
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            stream,
            stream_alias,
            stream_version,
            start_time
        )
        
        # Assert
        assert mock_write_message.call_count == 1
        first_call = mock_write_message.call_args_list[0][0][0]
        assert first_call.stream == stream_alias  # Should use alias
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_stops_at_total_row(self, mock_write_message, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test that syncing stops when 'total' row is encountered."""
        # Arrange
        header_row = (None, "Name", "Amount")
        data_rows = [
            (None, "Deal 1", 1000),
            (None, "Deal 2", 2000),
            (None, "total", 3000),  # Total row
            (None, "Deal 3", 999),  # This should not be synced
        ]
        excel_content = create_excel_file(header_row, data_rows, include_total=False)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - only 2 records should be written (Deal 1 and Deal 2)
        assert mock_write_message.call_count == 2
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_handles_empty_report(self, mock_write_message, mock_sf_client, sample_catalog_entry,
                                      load_fixture, create_excel_from_fixture):
        """Test syncing a report with only headers (no data) using fixtures."""
        # Arrange - Load fixture data
        excel_data = load_fixture("streams/report/excel_data/empty_report")
        excel_content = create_excel_from_fixture(excel_data)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - no records should be written
        assert mock_write_message.call_count == 0
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_with_null_values(self, mock_write_message, mock_sf_client, sample_catalog_entry,
                                   load_fixture, create_excel_from_fixture):
        """Test syncing handles null/None values correctly using fixtures."""
        # Arrange - Load fixture data
        excel_data = load_fixture("streams/report/excel_data/with_nulls")
        excel_content = create_excel_from_fixture(excel_data)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - 3 records should be written with null values preserved
        assert mock_write_message.call_count == 3
        
        first_record = mock_write_message.call_args_list[0][0][0].record
        assert first_record["Name"] == "Deal 1"
        assert first_record["Amount"] is None
        
        second_record = mock_write_message.call_args_list[1][0][0].record
        assert second_record["Name"] == "Deal 2"
        assert second_record["Amount"] == 1500
        
        third_record = mock_write_message.call_args_list[2][0][0].record
        assert third_record["Name"] == "Deal 3"
        assert third_record["Status"] is None
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_preserves_data_types(self, mock_write_message, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test that openpyxl data types are preserved (numbers, dates, strings)."""
        # Arrange
        header_row = (None, "Name", "Amount", "Count", "Date")
        data_rows = [
            (None, "Deal 1", 1000.50, 5, datetime.datetime(2024, 1, 15)),
            (None, "Deal 2", 2500, 10, datetime.datetime(2024, 2, 20)),
        ]
        excel_content = create_excel_file(header_row, data_rows)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - verify data types are preserved
        first_record = mock_write_message.call_args_list[0][0][0].record
        assert isinstance(first_record["Name"], str)
        assert isinstance(first_record["Amount"], float)
        assert first_record["Amount"] == 1000.50
        assert isinstance(first_record["Count"], int)
        assert first_record["Count"] == 5
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_filters_none_headers(self, mock_write_message, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test that columns with None headers are filtered out (not included in output)."""
        # Arrange
        header_row = (None, "Name", "Amount", None, "Status")  # Columns 0 and 3 have None headers
        data_rows = [
            ("RowID", "Deal 1", 1000, "Extra", "Open"),
        ]
        excel_content = create_excel_file(header_row, data_rows)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - None headers should be filtered out (line 624 in sync.py)
        first_record = mock_write_message.call_args_list[0][0][0].record
        assert None not in first_record  # None header should NOT be present
        assert first_record["Name"] == "Deal 1"
        assert first_record["Amount"] == 1000
        assert first_record["Status"] == "Open"
        # Verify only 3 keys (the non-None headers)
        assert len(first_record) == 3
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_case_insensitive_total_check(self, mock_write_message, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test that 'total' row check is case-insensitive."""
        # Arrange
        header_row = (None, "Name", "Amount")
        data_rows = [
            (None, "Deal 1", 1000),
            (None, "TOTAL", 1000),  # Uppercase TOTAL
        ]
        excel_content = create_excel_file(header_row, data_rows, include_total=False)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - only 1 record should be written (stops at TOTAL)
        assert mock_write_message.call_count == 1
    
    @patch('tap_salesforce.sync.singer.write_message')
    def test_sync_extracts_report_name_correctly(self, mock_write_message, mock_sf_client, 
                                                 catalog_entry_opportunity, create_excel_file):
        """Test that report name is extracted correctly from stream name using fixture."""
        # Arrange - Use opportunity catalog entry fixture
        catalog_entry = catalog_entry_opportunity
        
        # Use first field from catalog entry schema
        first_field = next(iter(catalog_entry["schema"]["properties"]))
        header_row = (None, first_field)
        data_rows = [(None, "Big Deal Corp")]
        excel_content = create_excel_file(header_row, data_rows)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            catalog_entry,
            catalog_entry["stream"],
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - verify report name was extracted (implicitly tested by successful execution)
        assert mock_write_message.call_count == 1
        
    def test_sync_builds_correct_url(self, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test that the correct Salesforce API URL is constructed."""
        # Arrange
        header_row = (None, "Name")
        data_rows = []
        excel_content = create_excel_file(header_row, data_rows)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - verify URL construction
        call_args = mock_sf_client._make_request.call_args
        url = call_args[0][1]
        assert url == "https://example.salesforce.com/services/data/v52.0/analytics/reports/00O1234567890ABC"
    
    def test_sync_includes_required_parameters(self, mock_sf_client, sample_catalog_entry, create_excel_file):
        """Test that all required parameters are included in the request."""
        # Arrange
        header_row = (None, "Name")
        data_rows = []
        excel_content = create_excel_file(header_row, data_rows)
        
        mock_response = MagicMock()
        mock_response.content = excel_content
        mock_sf_client._make_request.return_value = mock_response
        
        # Act
        sync_report_via_excel(
            mock_sf_client,
            sample_catalog_entry,
            "Report_TestReport",
            None,
            12345,
            get_aware_datetime()
        )
        
        # Assert - verify all required parameters
        call_args = mock_sf_client._make_request.call_args
        params = call_args[1]['params']
        
        assert params['export'] == 1
        assert params['enc'] == 'UTF-8'
        assert params['xf'] == 'xlsx'
        assert params['data'] == 2
        assert params['includeDetails'] is True
