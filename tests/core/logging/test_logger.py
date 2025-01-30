"""Unit tests for logging implementation."""

import logging
from unittest.mock import patch, MagicMock
import pytest

from dra.common.logging.logger import DRALogger

@pytest.fixture
def mock_logger():
    """Create a mock logger instance."""
    logger = MagicMock(spec=logging.Logger)
    # Explicitly create mock methods
    logger.debug = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    logger.critical = MagicMock()
    return logger

class TestDRALogger:
    """Test cases for DRALogger."""
    
    def test_get_logger(self):
        """Should return logger with context-aware methods."""
        mock = mock_logger()
        
        with patch('logging.getLogger', return_value=mock):
            wrapped_logger = DRALogger.get_logger("test")
            
            # Test logging with context
            wrapped_logger.info("test message", context={"key": "value"})
            
            # Verify context was added
            mock.info.assert_called_once()
            args, kwargs = mock.info.call_args
            assert args[0] == "test message"
            assert "extra" in kwargs
            assert "context" in kwargs["extra"]
            assert "[key:value]" in kwargs["extra"]["context"]
    
    def test_logging_without_context(self):
        """Should work correctly without context."""
        mock = mock_logger()
        
        with patch('logging.getLogger', return_value=mock):
            wrapped_logger = DRALogger.get_logger("test")
            
            wrapped_logger.info("test message")
            
            mock.info.assert_called_once_with("test message")
    
    @patch('dra.common.logging.logger.SecretsManager')
    def test_setup_with_azure(self, mock_secrets_manager):
        """Should configure logging with Azure when key is available."""
        mock_secrets_manager.get_secret.return_value = "test-key"
        
        with patch('logging.config.dictConfig') as mock_dict_config:
            DRALogger.setup(azure_key_name="test-key-name")
            
            # Verify dictConfig was called with correct configuration
            mock_dict_config.assert_called_once()
            config = mock_dict_config.call_args[0][0]
            assert 'azure' in config['handlers']
            assert config['handlers']['azure']['instrumentation_key'] == "test-key"
    
    def test_setup_without_azure(self):
        """Should configure basic logging without Azure."""
        with patch('logging.config.dictConfig') as mock_dict_config:
            DRALogger.setup()
            
            # Verify basic config was applied
            mock_dict_config.assert_called_once()
            config = mock_dict_config.call_args[0][0]
            assert 'console' in config['handlers']
            assert 'azure' not in config['handlers']
    
    def test_all_log_levels(self):
        """Should properly wrap all log levels."""
        mock = mock_logger()
        
        with patch('logging.getLogger', return_value=mock):
            wrapped_logger = DRALogger.get_logger("test")
            context = {"test": "value"}
            
            # Test all log levels
            wrapped_logger.debug("debug", context=context)
            wrapped_logger.info("info", context=context)
            wrapped_logger.warning("warning", context=context)
            wrapped_logger.error("error", context=context)
            wrapped_logger.critical("critical", context=context)
            
            # Verify all were called with context
            for method in [mock.debug, mock.info, 
                         mock.warning, mock.error, 
                         mock.critical]:
                method.assert_called_once()
                args, kwargs = method.call_args
                assert "extra" in kwargs
                assert "context" in kwargs["extra"]
                assert "[test:value]" in kwargs["extra"]["context"] 