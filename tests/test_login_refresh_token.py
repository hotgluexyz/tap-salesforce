from unittest.mock import MagicMock, patch

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
    }
    auth = _authenticator(tap_config)
    auth.update_access_token = MagicMock(side_effect=lambda: (
        setattr(auth, "access_token", "new-access"),
        tap_config.update({
            "access_token": "new-access",
            "instance_url": INSTANCE,
            "refresh_token": "new-token",
        }),
    ))
    sf = _sf(auth, tap_config)
    sf.login()

    assert sf.access_token == "new-access"
    assert sf.refresh_token == "new-token"
    assert tap_config["refresh_token"] == "new-token"


def test_login_skips_refresh_token_when_not_rotated():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
    }
    auth = _authenticator(tap_config)
    auth.update_access_token = MagicMock(side_effect=lambda: (
        setattr(auth, "access_token", "new-access"),
        tap_config.update({
            "access_token": "new-access",
            "instance_url": INSTANCE,
        }),
    ))
    sf = _sf(auth, tap_config)
    sf.login()

    assert sf.access_token == "new-access"
    assert sf.refresh_token == "old-token"
    assert tap_config["refresh_token"] == "old-token"


def test_login_force_invalidates_authenticator():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
    }
    auth = _authenticator(tap_config)
    auth.invalidate = MagicMock()
    auth.update_access_token = MagicMock(side_effect=lambda: (
        setattr(auth, "access_token", "new-access"),
        tap_config.update({"access_token": "new-access", "instance_url": INSTANCE}),
    ))
    sf = _sf(auth, tap_config)
    sf.login(force=True)

    auth.invalidate.assert_called_once_with()
    assert sf.access_token == "new-access"


def test_authenticator_persists_instance_url_and_rotated_refresh_token():
    tap_config = {
        "client_id": "cid",
        "client_secret": "secret",
        "refresh_token": "old-token",
    }
    auth = _authenticator(tap_config)
    response = MagicMock()
    response.json.return_value = {
        "access_token": "new-access",
        "instance_url": INSTANCE,
        "refresh_token": "new-token",
    }
    response.raise_for_status = MagicMock()

    with patch("tap_salesforce.auth.requests.post", return_value=response) as post:
        auth.update_access_token_locally()

    post.assert_called_once()
    assert auth.access_token == "new-access"
    assert auth.expires_in is None
    assert tap_config["access_token"] == "new-access"
    assert tap_config["instance_url"] == INSTANCE
    assert tap_config["refresh_token"] == "new-token"
