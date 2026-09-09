import json

import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.helpers._util import utc_now

from tap_salesforce.salesforce import validate_auth_config


class SalesforceOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Salesforce API.

    Supports the refresh_token and client_credentials grant types.

    Salesforce token lifetimes vary by org session / connected-app policy and often
    omit expires_in. Do not invent a default expiry — refresh on InvalidSessionId
    (or when expires_in is present and the SDK marks the token stale).
    """

    @property
    def oauth_request_body(self) -> dict:
        validate_auth_config(self.config)
        if self.config.get("refresh_token"):
            return {
                "grant_type": "refresh_token",
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "refresh_token": self.config["refresh_token"],
            }
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

    def invalidate(self) -> None:
        """Force the next auth check to refresh (e.g. InvalidSessionId)."""
        self.last_refreshed = None
        self.expires_in = None

    def update_access_token_locally(self) -> None:
        """Refresh locally and persist Salesforce instance_url when returned."""
        request_time = utc_now()
        token_response = requests.post(
            self.auth_endpoint,
            data=self.oauth_request_payload,
            auth=self.request_auth(),
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise InvalidCredentialsError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            ) from ex

        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = token_json.get("expires_in", self._default_expiration)
        if expires_in is None:
            self.logger.debug(
                "No expires_in in OAuth response; token will be refreshed on "
                "InvalidSessionId or similar auth failure."
            )
            self.expires_in = None
        else:
            self.expires_in = int(expires_in) + int(request_time.timestamp())

        self.last_refreshed = request_time
        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["expires_in"] = self.expires_in
        if token_json.get("instance_url"):
            self._tap._config["instance_url"] = token_json["instance_url"]
        if token_json.get("refresh_token"):
            self._tap.logger.info(
                "Latest refresh token: %s", token_json.get("refresh_token")
            )
            self._tap._config["refresh_token"] = token_json["refresh_token"]

        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
