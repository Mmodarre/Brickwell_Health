"""Tests for OAuth token caching across ZeroBus workers."""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from brickwell_health.streaming.token_cache import (
    _EXPIRY_BUFFER_SECONDS,
    CachingHeadersProvider,
    cleanup_token_cache,
)


def _mock_oauth_response(access_token: str = "test-token", expires_in: int = 3600):
    """Create a mock requests.Response for OAuth token fetch."""
    resp = MagicMock()
    resp.json.return_value = {
        "access_token": access_token,
        "expires_in": expires_in,
    }
    resp.raise_for_status = MagicMock()
    return resp


def _make_provider(tmp_path: Path) -> CachingHeadersProvider:
    """Create a CachingHeadersProvider pointed at tmp_path."""
    return CachingHeadersProvider(
        workspace_id="ws-123",
        workspace_url="https://test.cloud.databricks.com",
        table_name="catalog.schema.claim",
        client_id="app-id",
        client_secret="secret",
        cache_dir=tmp_path,
    )


class TestCachingHeadersProvider:
    def test_first_call_fetches_and_caches(self, tmp_path):
        """First get_headers() should fetch token and write cache file."""
        provider = _make_provider(tmp_path)

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("fresh-token", 3600)
            headers = provider.get_headers()

        assert ("authorization", "Bearer fresh-token") in headers
        assert ("x-databricks-zerobus-table-name", "catalog.schema.claim") in headers
        mock_post.assert_called_once()

        # Verify cache file was written
        cache_file = tmp_path / "catalog_schema_claim.json"
        assert cache_file.exists()
        data = json.loads(cache_file.read_text())
        assert data["access_token"] == "fresh-token"
        assert data["expires_at"] > time.time()

    def test_second_call_reads_from_cache(self, tmp_path):
        """Second get_headers() should read cached token, no HTTP call."""
        provider = _make_provider(tmp_path)

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("cached-token", 3600)

            # First call: fetches and caches
            provider.get_headers()
            assert mock_post.call_count == 1

            # Second call: should use cache
            headers = provider.get_headers()
            assert mock_post.call_count == 1  # No additional HTTP call

        assert ("authorization", "Bearer cached-token") in headers

    def test_expired_token_triggers_refresh(self, tmp_path):
        """Expired cache file should trigger fresh token fetch."""
        provider = _make_provider(tmp_path)

        # Write an expired cache entry
        cache_file = tmp_path / "catalog_schema_claim.json"
        cache_file.write_text(
            json.dumps(
                {
                    "access_token": "old-token",
                    "fetched_at": time.time() - 7200,
                    "expires_at": time.time() - 100,
                }
            )
        )

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("new-token", 3600)
            headers = provider.get_headers()

        assert ("authorization", "Bearer new-token") in headers
        mock_post.assert_called_once()

    def test_expiry_buffer_rejects_nearly_expired(self, tmp_path):
        """Token within expiry buffer should be treated as expired."""
        provider = _make_provider(tmp_path)

        # Write a cache entry that expires within the buffer window
        cache_file = tmp_path / "catalog_schema_claim.json"
        cache_file.write_text(
            json.dumps(
                {
                    "access_token": "about-to-expire",
                    "fetched_at": time.time() - 3500,
                    "expires_at": time.time() + (_EXPIRY_BUFFER_SECONDS - 10),
                }
            )
        )

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("refreshed", 3600)
            headers = provider.get_headers()

        assert ("authorization", "Bearer refreshed") in headers
        mock_post.assert_called_once()

    def test_corrupt_cache_falls_back_to_fetch(self, tmp_path):
        """Corrupt JSON in cache file should fall back to direct fetch."""
        provider = _make_provider(tmp_path)

        cache_file = tmp_path / "catalog_schema_claim.json"
        cache_file.write_text("not valid json {{{")

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("fallback-token", 3600)
            headers = provider.get_headers()

        assert ("authorization", "Bearer fallback-token") in headers
        mock_post.assert_called_once()

    def test_missing_cache_dir_created_on_write(self, tmp_path):
        """If cache_dir subdirectory doesn't exist, _write_cache creates it."""
        nested_dir = tmp_path / "sub" / "dir"
        provider = CachingHeadersProvider(
            workspace_id="ws-123",
            workspace_url="https://test.cloud.databricks.com",
            table_name="catalog.schema.claim",
            client_id="app-id",
            client_secret="secret",
            cache_dir=nested_dir,
        )

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("token", 3600)
            provider.get_headers()

        assert nested_dir.exists()
        assert (nested_dir / "catalog_schema_claim.json").exists()

    def test_file_io_error_falls_back_to_direct_fetch(self, tmp_path):
        """If file write fails, should still return a valid token."""
        provider = _make_provider(tmp_path)

        with (
            patch("brickwell_health.streaming.token_cache.requests.post") as mock_post,
            patch.object(provider, "_write_cache", side_effect=OSError("disk full")),
        ):
            mock_post.return_value = _mock_oauth_response("fallback", 3600)
            headers = provider.get_headers()

        assert ("authorization", "Bearer fallback") in headers

    def test_cross_worker_sharing(self, tmp_path):
        """Two providers sharing the same cache dir should share tokens."""
        provider1 = _make_provider(tmp_path)
        provider2 = _make_provider(tmp_path)

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("shared-token", 3600)

            # Provider 1 fetches and caches
            provider1.get_headers()
            assert mock_post.call_count == 1

            # Provider 2 reads from cache
            headers = provider2.get_headers()
            assert mock_post.call_count == 1

        assert ("authorization", "Bearer shared-token") in headers

    def test_fetch_includes_correct_authorization_details(self, tmp_path):
        """Verify the OIDC request body matches SDK's format."""
        provider = _make_provider(tmp_path)

        with patch("brickwell_health.streaming.token_cache.requests.post") as mock_post:
            mock_post.return_value = _mock_oauth_response("token", 3600)
            provider.get_headers()

        call_kwargs = mock_post.call_args
        url = call_kwargs[0][0]
        assert url == "https://test.cloud.databricks.com/oidc/v1/token"

        data = call_kwargs[1]["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["scope"] == "all-apis"
        assert "ws-123" in data["resource"]

        auth_details = json.loads(data["authorization_details"])
        assert len(auth_details) == 3
        assert auth_details[0]["object_type"] == "CATALOG"
        assert auth_details[2]["object_full_path"] == "catalog.schema.claim"


class TestCleanupTokenCache:
    def test_removes_directory_and_files(self, tmp_path):
        """cleanup_token_cache should remove the cache directory."""
        cache_dir = tmp_path / "token_cache"
        cache_dir.mkdir()
        (cache_dir / "catalog_schema_claim.json").write_text('{"token": "x"}')
        (cache_dir / "catalog_schema_invoice.json").write_text('{"token": "y"}')

        cleanup_token_cache(cache_dir)
        assert not cache_dir.exists()

    def test_handles_nonexistent_directory(self, tmp_path):
        """Should not raise when cache dir doesn't exist."""
        nonexistent = tmp_path / "does_not_exist"
        cleanup_token_cache(nonexistent)  # Should not raise
