"""
Integration tests for reference data loader.
"""

import pytest

from brickwell_health.reference.loader import ReferenceDataLoader


class TestReferenceDataLoader:
    """Integration tests for ReferenceDataLoader."""

    def test_load_products(self, test_reference: ReferenceDataLoader):
        """Should load products from JSON."""
        products = test_reference.get_products()

        assert len(products) > 0
        assert all("product_id" in p for p in products)

    def test_load_states(self, test_reference: ReferenceDataLoader):
        """Should load states from JSON."""
        states = test_reference.get_states()

        assert len(states) > 0
        assert any(s["state_code"] == "NSW" for s in states)

    def test_get_product_by_id(self, test_reference: ReferenceDataLoader):
        """Should retrieve product by ID."""
        product = test_reference.get_product_by_id(1)

        assert product is not None
        assert product["product_id"] == 1

    def test_get_state_by_code(self, test_reference: ReferenceDataLoader):
        """Should retrieve state by code."""
        state = test_reference.get_state_by_code("NSW")

        assert state is not None
        assert state["state_code"] == "NSW"

    def test_caching_works(self, test_reference: ReferenceDataLoader):
        """Multiple loads should use cache."""
        # Load twice
        products1 = test_reference.load("product.json")
        products2 = test_reference.load("product.json")

        # Should be same object (cached)
        assert products1 is products2

    def test_clear_cache(self, test_reference: ReferenceDataLoader):
        """clear_cache should invalidate cache."""
        products1 = test_reference.load("product.json")
        test_reference.clear_cache()
        products2 = test_reference.load("product.json")

        # Should be different objects after cache clear
        assert products1 is not products2

    def test_missing_file_raises_error(self, test_reference: ReferenceDataLoader):
        """Loading missing file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            test_reference.load("nonexistent.json")
