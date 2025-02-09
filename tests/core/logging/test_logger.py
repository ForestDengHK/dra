"""Unit tests for logging implementation."""
import pytest
import logging
from unittest.mock import patch, MagicMock, call

from dra.common.logging.logger import DRALogger


@pytest.fixture
def mock_logger():
    """Create a mock logger instance."""
    return MagicMock(spec=logging.Logger)


class TestDRALogger:
    """Test cases for DRALogger."""

    def test_get_logger(self, mock_logger):
        """Should return logger with context-aware methods."""
        # Patch where getLogger is called in DRALogger.get_logger
        with patch('dra.common.logging.logger.logging.getLogger', return_value=mock_logger):
            wrapped_logger = DRALogger.get_logger("test")
            
            # Test logging with context
            wrapped_logger.info("test message", context={"key": "value"})
        
        # Verify the underlying mock logged as expected
        # The wrap_log() function calls the original .info(...) with extra={...}
        assert mock_logger.mock_calls == [
            call.info("test message", extra={'context': '[key:value]'})
        ]

    def test_logging_without_context(self, mock_logger):
        """Should work correctly without context."""
        with patch('dra.common.logging.logger.logging.getLogger', return_value=mock_logger):
            wrapped_logger = DRALogger.get_logger("test")
            wrapped_logger.info("test message")

        # Verify the underlying mock call
        assert mock_logger.mock_calls == [
            call.info("test message")
        ]

    @patch('dra.common.logging.logger.SecretsManager')
    def test_setup_with_azure(self, mock_secrets_manager):
        """Should configure logging with Azure when key is available."""
        mock_secrets_manager.get_secret.return_value = "test-key"

        # Patch dictConfig exactly where it's used in logger.py
        with patch('dra.common.logging.logger.logging.config.dictConfig') as mock_dict_config:
            DRALogger.setup(azure_key_name="test-key-name")

            # Verify dictConfig was called with correct configuration
            mock_dict_config.assert_called_once()
            config = mock_dict_config.call_args[0][0]
            assert 'azure' in config['handlers']
            assert config['handlers']['azure']['instrumentation_key'] == "test-key"

    def test_setup_without_azure(self):
        """Should configure basic logging without Azure."""
        # Patch dictConfig exactly where it's used in logger.py
        with patch('dra.common.logging.logger.logging.config.dictConfig') as mock_dict_config:
            DRALogger.setup()

            # Verify basic config was applied
            mock_dict_config.assert_called_once()
            config = mock_dict_config.call_args[0][0]
            assert 'console' in config['handlers']
            assert 'azure' not in config['handlers']

    def test_all_log_levels(self, mock_logger):
        """Should properly wrap all log levels."""
        with patch('dra.common.logging.logger.logging.getLogger', return_value=mock_logger):
            wrapped_logger = DRALogger.get_logger("test")
            context = {"test": "value"}

            # Test all log levels
            wrapped_logger.debug("debug", context=context)
            wrapped_logger.info("info", context=context)
            wrapped_logger.warning("warning", context=context)
            wrapped_logger.error("error", context=context)
            wrapped_logger.critical("critical", context=context)

        # Verify the calls (in order)
        assert mock_logger.mock_calls == [
            call.debug("debug", extra={'context': '[test:value]'}),
            call.info("info", extra={'context': '[test:value]'}),
            call.warning("warning", extra={'context': '[test:value]'}),
            call.error("error", extra={'context': '[test:value]'}),
            call.critical("critical", extra={'context': '[test:value]'})
        ]
