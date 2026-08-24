"""Tests for report catalog generation during discover."""

from tap_salesforce import generate_report_schema


def test_generate_report_schema_includes_label_mapping_in_stream_meta(load_fixture):
    """Report streams expose label-to-API-name mapping in stream_meta."""
    report_describe = load_fixture("streams/report/api_responses/report_describe")
    report = {
        "Id": report_describe["reportMetadata"]["id"],
        "Name": report_describe["reportMetadata"]["name"],
        "DeveloperName": report_describe["reportMetadata"]["developerName"],
        "FolderName": "Public Reports",
    }
    detail_column_info = report_describe["reportExtendedMetadata"]["detailColumnInfo"]

    entry = generate_report_schema(detail_column_info, report)

    assert entry["stream_meta"]["labelToApiName"] == {
        "Opportunity Name": "OPPORTUNITY_NAME",
        "Amount": "AMOUNT",
        "Stage": "STAGE_NAME",
        "Close Date": "CLOSE_DATE",
    }
    assert set(entry["schema"]["properties"]) == set(entry["stream_meta"]["labelToApiName"])


def test_generate_report_schema_maps_html_detail_columns_to_string():
    """Report detailColumnInfo html fields map to nullable string schema."""
    report = {
        "Id": "00O1234567890ABC",
        "Name": "Test Report",
        "DeveloperName": "Test_Report",
        "FolderName": "Public Reports",
    }
    detail_column_info = {
        "HTML_COLUMN": {
            "label": "Rich Text Column",
            "dataType": "html",
        }
    }

    entry = generate_report_schema(detail_column_info, report)

    assert entry["schema"]["properties"]["Rich Text Column"] == {
        "type": ["null", "string"]
    }
