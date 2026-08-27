from unittest.mock import MagicMock

from tap_salesforce.salesforce import Salesforce

START = "2020-01-01T00:00:00Z"


def _sf(**kwargs):
    defaults = dict(
        refresh_token="old-token",
        sf_client_id="cid",
        sf_client_secret="secret",
        instance_url="https://example.my.salesforce.com",
        default_start_date=START,
        api_type="REST",
    )
    defaults.update(kwargs)
    return Salesforce(**defaults)


def test_login_persists_rotated_refresh_token_to_tap_config():
    tap_config = {"refresh_token": "old-token"}
    sf = _sf(tap_config=tap_config)
    sf._make_request = MagicMock(return_value=MagicMock(json=lambda: {
        "access_token": "new-access",
        "instance_url": "https://example.my.salesforce.com",
        "refresh_token": "new-token",
    }))
    try:
        sf.login()
    finally:
        sf.login_timer.cancel()

    assert sf.refresh_token == "new-token"
    assert tap_config["refresh_token"] == "new-token"


def test_login_skips_refresh_token_when_not_rotated():
    tap_config = {"refresh_token": "old-token"}
    sf = _sf(tap_config=tap_config)
    sf._make_request = MagicMock(return_value=MagicMock(json=lambda: {
        "access_token": "new-access",
        "instance_url": "https://example.my.salesforce.com",
    }))
    try:
        sf.login()
    finally:
        sf.login_timer.cancel()

    assert sf.refresh_token == "old-token"
    assert tap_config["refresh_token"] == "old-token"
