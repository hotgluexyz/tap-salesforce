from hotglue_singer_sdk.authenticators import OAuthAuthenticator

class SalesforceOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Salesforce API.

    Implements the refresh_token grant type used by the QuickBooks Online and Salesforce APIs.
    """

    @property
    def oauth_request_body(self) -> dict:
        return {
            "grant_type": "refresh_token",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "refresh_token": self.config["refresh_token"],
        }
