"""API-key authentication for the HTTP API.

Keys are configured as client name -> secret (``Settings.api_keys``), so each caller gets its own
key and one can be rotated or revoked without touching the others. HTTP-free on purpose: the API
maps a failed lookup to a 401; this module only answers "which client presented this key?".
"""

import hmac
from collections.abc import Mapping

from pydantic import SecretStr


class ApiKeyAuthenticator:
    def __init__(self, keys: Mapping[str, SecretStr]) -> None:
        self._keys = {client: key.get_secret_value().encode() for client, key in keys.items()}

    @property
    def enabled(self) -> bool:
        """False when no keys are configured — the API is then open (dev only)."""
        return bool(self._keys)

    def identify(self, presented: str | None) -> str | None:
        """The client name the presented key belongs to, or None if it matches no key.

        Every key is compared, in constant time, so the response time leaks neither which client
        nor how much of a key matched.
        """
        if not presented:
            return None
        candidate = presented.encode()
        matched: str | None = None
        for client, key in self._keys.items():
            if hmac.compare_digest(candidate, key):
                matched = client
        return matched
