from unittest.mock import MagicMock

from tap_salesforce.auth import SalesforceOAuthAuthenticator
from tap_salesforce.salesforce import Salesforce

START = "2020-01-01T00:00:00Z"
INSTANCE = "https://example.my.salesforce.com"


class _FakeTap:
    def __init__(self, config, config_file=None):
        self._config = config
        self.config = config
        self.config_file = config_file
        self.logger = MagicMock()
        self.name = "tap-salesforce"


class _FakeStream:
    def __init__(self, tap):
        self._tap = tap
        self.logger = tap.logger
        self.tap_name = tap.name
        self.config = tap.config


def _authenticator(tap_config, **kwargs):
    tap = _FakeTap(tap_config)
    return SalesforceOAuthAuthenticator(
        stream=_FakeStream(tap),
        auth_endpoint=f"{INSTANCE}/services/oauth2/token",
        **kwargs,
    )


def _sf(authenticator, tap_config, **kwargs):
    defaults = dict(
        refresh_token=tap_config.get("refresh_token", "old-token"),
        sf_client_id="cid",
        sf_client_secret="secret",
        instance_url=INSTANCE,
        default_start_date=START,
        api_type="REST",
        tap_config=tap_config,
        authenticator=authenticator,
    )
    defaults.update(kwargs)
    return Salesforce(**defaults)


def test_login_persists_rotated_refresh_token_to_tap_config():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
        "instance_url": INSTANCE,
    }
    auth = _authenticator(tap_config)
    auth.update_access_token = MagicMock(side_effect=lambda: (
        setattr(auth, "access_token", "new-access"),
        tap_config.update({
            "access_token": "new-access",
            "refresh_token": "new-token",
        }),
    ))
    sf = _sf(auth, tap_config)
    sf.login()

    assert sf.access_token == "new-access"
    assert sf.refresh_token == "new-token"
    assert tap_config["refresh_token"] == "new-token"
    assert sf.instance_url == INSTANCE


def test_login_skips_refresh_token_when_not_rotated():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
        "instance_url": INSTANCE,
    }
    auth = _authenticator(tap_config)
    auth.update_access_token = MagicMock(side_effect=lambda: (
        setattr(auth, "access_token", "new-access"),
        tap_config.update({"access_token": "new-access"}),
    ))
    sf = _sf(auth, tap_config)
    sf.login()

    assert sf.access_token == "new-access"
    assert sf.refresh_token == "old-token"
    assert tap_config["refresh_token"] == "old-token"
    assert sf.instance_url == INSTANCE


def test_login_force_invalidates_authenticator():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
        "instance_url": INSTANCE,
    }
    auth = _authenticator(tap_config)
    auth.invalidate = MagicMock()
    auth.update_access_token = MagicMock(side_effect=lambda: (
        setattr(auth, "access_token", "new-access"),
        tap_config.update({"access_token": "new-access"}),
    ))
    sf = _sf(auth, tap_config)
    sf.login(force=True)

    auth.invalidate.assert_called_once_with()
    assert sf.access_token == "new-access"


def test_oauth_request_body_refresh_token_grant():
    auth = _authenticator({
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "rt",
        "instance_url": INSTANCE,
    })
    assert auth.oauth_request_body["grant_type"] == "refresh_token"


def test_oauth_request_body_uses_rotated_refresh_token_from_tap_config():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
        "instance_url": INSTANCE,
    }
    auth = _authenticator(tap_config)
    assert auth.config["refresh_token"] == "old-token"

    tap_config["refresh_token"] = "new-token"

    assert auth.config["refresh_token"] == "old-token"
    assert auth.oauth_request_body["refresh_token"] == "new-token"


def test_oauth_request_body_client_credentials_grant():
    auth = _authenticator({
        "client_id": "cid",
        "client_secret": "secret",
        "instance_url": INSTANCE,
    })
    assert auth.oauth_request_body["grant_type"] == "client_credentials"
