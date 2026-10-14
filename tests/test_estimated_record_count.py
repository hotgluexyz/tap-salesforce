from unittest.mock import MagicMock

from tap_salesforce.salesforce import Salesforce


def _salesforce():
    return Salesforce(
        sf_client_id="client-id",
        sf_client_secret="client-secret",
        instance_url="https://example.my.salesforce.com",
        default_start_date="2020-01-01T00:00:00Z",
        api_type="BULK",
    )


def test_estimated_record_count_uses_sync_bookmark_filter():
    sf = _salesforce()
    response = MagicMock()
    response.json.return_value = {"totalSize": 3}
    sf._make_request = MagicMock(return_value=response)
    catalog_entry = {
        "stream": "Account",
        "tap_stream_id": "Account",
        "metadata": [{
            "breadcrumb": [],
            "metadata": {"replication-key": "SystemModstamp"},
        }],
    }
    state = {"bookmarks": {"Account": {"SystemModstamp": "2024-01-01T00:00:00Z"}}}

    assert sf.get_estimated_record_count(catalog_entry, state) == 3
    assert sf._make_request.call_args.kwargs["params"] == {
        "q": "SELECT COUNT() FROM Account WHERE SystemModstamp > 2024-01-01T00:00:00Z"
    }


def test_estimated_record_count_soft_fails_for_invalid_response():
    sf = _salesforce()
    response = MagicMock()
    response.json.return_value = {"totalSize": "not-a-count"}
    sf._make_request = MagicMock(return_value=response)
    catalog_entry = {
        "stream": "Account",
        "tap_stream_id": "Account",
        "metadata": [{"breadcrumb": [], "metadata": {}}],
    }

    assert sf.get_estimated_record_count(catalog_entry, {"bookmarks": {}}) is None
