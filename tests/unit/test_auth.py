"""ApiKeyAuthenticator: which configured client, if any, a presented key belongs to."""

from pydantic import SecretStr

from orchestrator.auth import ApiKeyAuthenticator

KEYS = {"ui": SecretStr("ui-key"), "portal": SecretStr("portal-key")}


def test_identifies_the_client_a_key_belongs_to() -> None:
    authenticator = ApiKeyAuthenticator(KEYS)
    assert authenticator.identify("ui-key") == "ui"
    assert authenticator.identify("portal-key") == "portal"


def test_unknown_missing_or_empty_key_matches_no_client() -> None:
    authenticator = ApiKeyAuthenticator(KEYS)
    assert authenticator.identify("ui-key-but-longer") is None
    assert authenticator.identify(None) is None
    assert authenticator.identify("") is None


def test_non_ascii_key_is_rejected_not_raised() -> None:
    # hmac.compare_digest raises TypeError on non-ASCII str; comparing bytes avoids that.
    assert ApiKeyAuthenticator(KEYS).identify("ключ") is None


def test_enabled_only_when_keys_are_configured() -> None:
    assert ApiKeyAuthenticator(KEYS).enabled
    assert not ApiKeyAuthenticator({}).enabled
