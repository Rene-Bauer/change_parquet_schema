"""Tests for parquet_transform.storage — _extract_blob_size (no Azure connection needed)."""
import pytest

from parquet_transform.storage import _extract_blob_size


class _BlobWithSize:
    def __init__(self, size):
        self.size = size


class _BlobWithPropertiesSize:
    class _Props:
        def __init__(self, size):
            self.size = size
    def __init__(self, size):
        self.properties = self._Props(size)


class _BlobWithPropertiesContentLength:
    class _Props:
        def __init__(self, length):
            self.content_length = length
    def __init__(self, length):
        self.properties = self._Props(length)


class _BlobWithNoUsableAttribute:
    pass


class _BlobWithNoneSize:
    size = None


class _BlobWithNegativeSize:
    size = -5


class TestExtractBlobSize:
    def test_reads_size_attribute(self):
        assert _extract_blob_size(_BlobWithSize(1024)) == 1024

    def test_reads_properties_size(self):
        assert _extract_blob_size(_BlobWithPropertiesSize(512)) == 512

    def test_reads_properties_content_length(self):
        assert _extract_blob_size(_BlobWithPropertiesContentLength(256)) == 256

    def test_returns_minus_one_when_no_attribute_found(self):
        assert _extract_blob_size(_BlobWithNoUsableAttribute()) == -1

    def test_returns_minus_one_when_size_is_none(self):
        assert _extract_blob_size(_BlobWithNoneSize()) == -1

    def test_zero_size_is_valid(self):
        assert _extract_blob_size(_BlobWithSize(0)) == 0

    def test_prefers_size_attribute_over_properties(self):
        """When both .size and .properties.size exist, .size takes priority."""
        class _Both:
            size = 100
            class properties:
                size = 999
        assert _extract_blob_size(_Both()) == 100

    def test_negative_size_falls_through_to_next_candidate(self):
        """A negative .size should not be returned; fall back to next candidate."""
        assert _extract_blob_size(_BlobWithNegativeSize()) == -1
