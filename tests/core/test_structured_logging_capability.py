"""Tests for structured logging capability functionality in Singer SDK."""

from __future__ import annotations

import pytest

from singer_sdk import Tap, Target
from singer_sdk.helpers.capabilities import PluginCapabilities, TapCapabilities


class SampleTapWithStructuredLogging(Tap):
    """Sample tap with structured logging capability."""

    name = "tap-sample-structured"
    capabilities = (
        TapCapabilities.CATALOG,
        TapCapabilities.DISCOVER,
        PluginCapabilities.STRUCTURED_LOGGING,
    )


class SampleTapWithoutStructuredLogging(Tap):
    """Sample tap without structured logging capability."""

    name = "tap-sample-no-structured"
    capabilities = (
        TapCapabilities.CATALOG,
        TapCapabilities.DISCOVER,
    )


class SampleTargetWithStructuredLogging(Target):
    """Sample target with structured logging capability."""

    name = "target-sample-structured"
    capabilities = (
        PluginCapabilities.ABOUT,
        PluginCapabilities.STREAM_MAPS,
        PluginCapabilities.STRUCTURED_LOGGING,
    )


class SampleTargetWithoutStructuredLogging(Target):
    """Sample target without structured logging capability."""

    name = "target-sample-no-structured"
    capabilities = (
        PluginCapabilities.ABOUT,
        PluginCapabilities.STREAM_MAPS,
    )


class TestStructuredLoggingCapability:
    """Test structured logging capability in Singer SDK plugins."""

    def test_tap_with_structured_logging_capability(self):
        """Test that tap correctly declares STRUCTURED_LOGGING capability."""
        tap = SampleTapWithStructuredLogging()

        # Check that the capability is in the capabilities list
        assert PluginCapabilities.STRUCTURED_LOGGING in tap.capabilities

        # Check that it appears in the about info
        about_info = tap._get_about_info()
        assert "structured-logging" in about_info.capabilities

    def test_tap_without_structured_logging_capability(self):
        """Test that tap without structured logging doesn't include the capability."""
        tap = SampleTapWithoutStructuredLogging()

        # Check that the capability is NOT in the capabilities list
        assert PluginCapabilities.STRUCTURED_LOGGING not in tap.capabilities

        # Check that it doesn't appear in the about info
        about_info = tap._get_about_info()
        assert "structured-logging" not in about_info.capabilities

    def test_target_with_structured_logging_capability(self):
        """Test that target correctly declares STRUCTURED_LOGGING capability."""
        target = SampleTargetWithStructuredLogging()

        # Check that the capability is in the capabilities list
        assert PluginCapabilities.STRUCTURED_LOGGING in target.capabilities

        # Check that it appears in the about info
        about_info = target._get_about_info()
        assert "structured-logging" in about_info.capabilities

    def test_target_without_structured_logging_capability(self):
        """Test that target without structured logging doesn't have the capability."""
        target = SampleTargetWithoutStructuredLogging()

        # Check that the capability is NOT in the capabilities list
        assert PluginCapabilities.STRUCTURED_LOGGING not in target.capabilities

        # Check that it doesn't appear in the about info
        about_info = target._get_about_info()
        assert "structured-logging" not in about_info.capabilities

    def test_structured_logging_capability_serialization(self):
        """Test that STRUCTURED_LOGGING capability is correctly serialized."""
        tap = SampleTapWithStructuredLogging()
        about_info = tap._get_about_info()

        # Convert to dict and check serialization
        about_dict = about_info.to_dict()
        assert "structured-logging" in about_dict["capabilities"]

    def test_multiple_capabilities_including_structured_logging(self):
        """Test plugin with multiple capabilities including structured logging."""
        tap = SampleTapWithStructuredLogging()

        expected_capabilities = [
            "catalog",
            "discover",
            "structured-logging",
        ]

        about_info = tap._get_about_info()
        actual_capabilities = about_info.capabilities

        # Check that all expected capabilities are present
        for capability in expected_capabilities:
            assert capability in actual_capabilities

        # Check that structured-logging is specifically included
        assert "structured-logging" in actual_capabilities

    @pytest.mark.parametrize(
        "plugin_class,should_have_capability",
        [
            (SampleTapWithStructuredLogging, True),
            (SampleTapWithoutStructuredLogging, False),
            (SampleTargetWithStructuredLogging, True),
            (SampleTargetWithoutStructuredLogging, False),
        ],
    )
    def test_structured_logging_capability_parametrized(
        self, plugin_class, should_have_capability
    ):
        """Test structured logging capability detection for different plugin types."""
        plugin = plugin_class()
        about_info = plugin._get_about_info()

        if should_have_capability:
            assert PluginCapabilities.STRUCTURED_LOGGING in plugin.capabilities
            assert "structured-logging" in about_info.capabilities
        else:
            assert PluginCapabilities.STRUCTURED_LOGGING not in plugin.capabilities
            assert "structured-logging" not in about_info.capabilities

    def test_structured_logging_capability_constant_value(self):
        """Test that STRUCTURED_LOGGING capability has the expected constant value."""
        # Verify the constant is defined and has expected value
        assert hasattr(PluginCapabilities, "STRUCTURED_LOGGING")
        assert PluginCapabilities.STRUCTURED_LOGGING.value == "structured-logging"

    def test_capability_enum_includes_structured_logging(self):
        """Test that PluginCapabilities enum includes STRUCTURED_LOGGING."""
        all_capabilities = list(PluginCapabilities)
        assert PluginCapabilities.STRUCTURED_LOGGING in all_capabilities

    def test_about_info_format_with_structured_logging(self):
        """Test that about info is formatted with structured logging capability."""
        tap = SampleTapWithStructuredLogging()
        about_info = tap._get_about_info()

        # Test text format includes structured-logging
        text_output = about_info.to_text()
        assert "structured-logging" in text_output

        # Test dict format includes structured-logging
        dict_output = about_info.to_dict()
        assert "structured-logging" in dict_output["capabilities"]
