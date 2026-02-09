"""
File-based OAuth token cache for cross-process token sharing.

Implements the ZeroBus SDK HeadersProvider interface with file-based
caching to avoid redundant OAuth token fetches across worker processes.
"""

import fcntl
import json
import shutil
import time
from pathlib import Path

import requests
import structlog
from zerobus.sdk.shared.headers_provider import HeadersProvider

logger = structlog.get_logger()

# Safety margin: refresh token when less than this many seconds remain
_EXPIRY_BUFFER_SECONDS = 120

# Default token lifetime assumption when expires_in is missing from response
_DEFAULT_TOKEN_LIFETIME_SECONDS = 3600


class CachingHeadersProvider(HeadersProvider):
    """
    HeadersProvider that caches OAuth tokens to shared filesystem.

    On get_headers():
    1. Try to read a valid (non-expired) token from cache file
    2. If cache miss/expired/corrupt, fetch new token via direct HTTP POST
    3. Write new token + expiry to cache file
    4. If any file I/O fails, fall back to direct fetch (fail-safe)

    File locking uses fcntl.flock() for cross-process safety.
    """

    def __init__(
        self,
        workspace_id: str,
        workspace_url: str,
        table_name: str,
        client_id: str,
        client_secret: str,
        cache_dir: Path,
    ) -> None:
        self._workspace_id = workspace_id
        self._workspace_url = workspace_url.rstrip("/")
        self._table_name = table_name
        self._client_id = client_id
        self._client_secret = client_secret
        self._cache_dir = cache_dir

        # table_name is "catalog.schema.table" -> sanitize to filename
        safe_name = table_name.replace(".", "_")
        self._cache_file = cache_dir / f"{safe_name}.json"

    def get_headers(self) -> list[tuple[str, str]]:
        """Return authorization headers, using cached token when possible."""
        token = self._get_token()
        return [
            ("authorization", f"Bearer {token}"),
            ("x-databricks-zerobus-table-name", self._table_name),
        ]

    def _get_token(self) -> str:
        """Get a valid token: from cache if possible, else fetch fresh."""
        # Step 1: Try cache
        try:
            cached = self._read_cache()
            if cached:
                logger.debug("token_cache_hit", table=self._table_name)
                return cached
        except Exception:
            pass

        # Step 2: Fetch fresh token
        token, expires_at = self._fetch_token_with_expiry()

        # Step 3: Write to cache (best effort)
        try:
            self._write_cache(token, expires_at)
            logger.debug("token_cache_written", table=self._table_name)
        except Exception as e:
            logger.debug("token_cache_write_failed", error=str(e))

        return token

    def _read_cache(self) -> str | None:
        """Read token from cache file if valid. Returns None on miss."""
        if not self._cache_file.exists():
            return None

        with open(self._cache_file) as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                data = json.load(f)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        expires_at = data.get("expires_at", 0)
        if time.time() >= (expires_at - _EXPIRY_BUFFER_SECONDS):
            return None

        return data.get("access_token")

    def _write_cache(self, token: str, expires_at: float) -> None:
        """Write token to cache file with exclusive lock."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        data = {
            "access_token": token,
            "fetched_at": time.time(),
            "expires_at": expires_at,
        }

        with open(self._cache_file, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                json.dump(data, f)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _fetch_token_with_expiry(self) -> tuple[str, float]:
        """
        Fetch OAuth token via direct HTTP POST to capture expires_in.

        Replicates the SDK's get_zerobus_token() logic but preserves
        the expires_in field from the OIDC response.
        """
        parts = self._table_name.split(".")
        catalog_name = parts[0]
        schema_name = parts[1]

        authorization_details = [
            {
                "type": "unity_catalog_privileges",
                "privileges": ["USE CATALOG"],
                "object_type": "CATALOG",
                "object_full_path": catalog_name,
            },
            {
                "type": "unity_catalog_privileges",
                "privileges": ["USE SCHEMA"],
                "object_type": "SCHEMA",
                "object_full_path": f"{catalog_name}.{schema_name}",
            },
            {
                "type": "unity_catalog_privileges",
                "privileges": ["SELECT", "MODIFY"],
                "object_type": "TABLE",
                "object_full_path": self._table_name,
            },
        ]

        url = f"{self._workspace_url}/oidc/v1/token"
        data = {
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "resource": f"api://databricks/workspaces/{self._workspace_id}/zerobusDirectWriteApi",
            "authorization_details": json.dumps(authorization_details),
        }

        response = requests.post(
            url, auth=(self._client_id, self._client_secret), data=data
        )
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", _DEFAULT_TOKEN_LIFETIME_SECONDS)
        expires_at = time.time() + expires_in

        return access_token, expires_at


def cleanup_token_cache(cache_dir: Path) -> None:
    """Remove token cache directory and all cache files."""
    if cache_dir.exists():
        shutil.rmtree(cache_dir, ignore_errors=True)
