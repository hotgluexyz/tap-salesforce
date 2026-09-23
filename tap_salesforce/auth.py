from hotglue_singer_sdk.authenticators import OAuthAuthenticator


class SalesforceOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Salesforce API.

    Supports the refresh_token and client_credentials grant types.
    """

    @property
    def oauth_request_body(self) -> dict:
        if self._tap._config.get("refresh_token"):
            return {
                "grant_type": "refresh_token",
                "client_id": self._tap._config["client_id"],
                "client_secret": self._tap._config["client_secret"],
                "refresh_token": self._tap._config["refresh_token"],
            }
        return {
            "grant_type": "client_credentials",
            "client_id": self._tap._config["client_id"],
            "client_secret": self._tap._config["client_secret"],
        }

