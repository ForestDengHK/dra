"""Unit tests for logging handlers."""

import logging
from unittest.mock import Mock, patch
import pytest
from opencensus.ext.azure.log_exporter import AzureLogHandler

from dra.common.logging.handlers import (
    AzureLogHandlerWithContext,
    ConsoleHandlerWithContext
)
from dra.common.logging.context import ContextManager, LoggingContext

@pytest.fixture
def mock_record():
    """Create a mock log record."""
    record = Mock(spec=logging.LogRecord)
    record.msg = "test message"
    record.levelno = logging.INFO
    record.levelname = "INFO"
    record.pathname = "test.py"
    record.lineno = 1
    record.args = ()
    record.exc_info = None
    return record

@pytest.fixture
def mock_azure_handler():
    """Create a mock Azure handler."""
    return Mock(spec=AzureLogHandler)

@pytest.mark.usefixtures('mock_validate_key')
class TestAzureLogHandlerWithContext:
    """Test cases for AzureLogHandlerWithContext."""
    
    @pytest.fixture(autouse=True)
    def mock_validate_key(self):
        """Mock the instrumentation key validation."""
        with patch('opencensus.ext.azure.common.utils.validate_instrumentation_key'):
            yield
    
    def test_init(self):
        """Should initialize correctly."""
        handler = AzureLogHandlerWithContext(instrumentation_key="test-key")
        assert handler is not None
        assert handler.options.instrumentation_key == "test-key"
    
    def test_emit_with_context(self, mock_record):
        """Should include context in custom dimensions."""
        with patch.object(AzureLogHandler, 'emit') as mock_emit:
            handler = AzureLogHandlerWithContext(instrumentation_key="test-key")
            
            # Set context
            context = LoggingContext(
                pipeline_name="test-pipeline",
                job_name="test-job"
            )
            ContextManager.set_context(context)
            
            # Emit log
            handler.emit(mock_record)
            
            # Verify context was added to custom dimensions
            mock_emit.assert_called_once()
            record = mock_emit.call_args[0][0]
            assert hasattr(record, 'custom_dimensions')
            assert record.custom_dimensions['pipeline_name'] == "test-pipeline"
            assert record.custom_dimensions['job_name'] == "test-job"
    
    def test_emit_without_context(self, mock_record):
        """Should work without context."""
        with patch.object(AzureLogHandler, 'emit') as mock_emit:
            handler = AzureLogHandlerWithContext(instrumentation_key="test-key")
            
            # Clear context
            ContextManager._context.current = None
            
            # Emit log
            handler.emit(mock_record)
            
            # Verify basic emit still works
            mock_emit.assert_called_once()
            record = mock_emit.call_args[0][0]
            assert not hasattr(record, 'custom_dimensions')
    
    def test_emit_with_existing_dimensions(self, mock_record):
        """Should merge with existing custom dimensions."""
        with patch.object(AzureLogHandler, 'emit') as mock_emit:
            handler = AzureLogHandlerWithContext(instrumentation_key="test-key")
            
            # Set context
            context = LoggingContext(pipeline_name="test-pipeline")
            ContextManager.set_context(context)
            
            # Add existing dimensions
            mock_record.custom_dimensions = {'existing': 'value'}
            
            # Emit log
            handler.emit(mock_record)
            
            # Verify dimensions were merged
            mock_emit.assert_called_once()
            record = mock_emit.call_args[0][0]
            assert record.custom_dimensions['existing'] == 'value'
            assert record.custom_dimensions['pipeline_name'] == "test-pipeline"
    
    def test_flush(self):
        """Should properly flush the handler."""
        with patch.object(AzureLogHandler, 'flush') as mock_flush:
            handler = AzureLogHandlerWithContext(instrumentation_key="test-key")
            
            handler.flush()
            
            mock_flush.assert_called_once()
    
    def test_flush_with_timeout(self):
        """Should handle flush with timeout."""
        handler = AzureLogHandlerWithContext(instrumentation_key="test-key")
        handler._queue = Mock()
        handler._queue.put = Mock()
        
        with patch.object(AzureLogHandler, 'flush') as mock_flush:
            handler.flush(timeout=5)
            
            mock_flush.assert_called_once()
            handler._queue.put.assert_called_once()

class TestConsoleHandlerWithContext:
    """Test cases for ConsoleHandlerWithContext."""
    
    def test_emit_with_context(self, mock_record):
        """Should include context in log record."""
        with patch.object(logging.StreamHandler, 'emit') as mock_emit:
            handler = ConsoleHandlerWithContext()
            
            # Set context
            context = LoggingContext(
                pipeline_name="test-pipeline",
                job_name="test-job"
            )
            ContextManager.set_context(context)
            
            # Emit log
            handler.emit(mock_record)
            
            # Verify context was added
            mock_emit.assert_called_once()
            record = mock_emit.call_args[0][0]
            assert hasattr(record, 'context_str')
            assert '[pipeline_name:test-pipeline]' in record.context_str
            assert '[job_name:test-job]' in record.context_str
    
    def test_emit_without_context(self, mock_record):
        """Should work without context."""
        with patch.object(logging.StreamHandler, 'emit') as mock_emit:
            handler = ConsoleHandlerWithContext()
            
            # Clear context
            ContextManager._context.current = None
            
            # Emit log
            handler.emit(mock_record)
            
            # Verify basic emit still works
            mock_emit.assert_called_once()
            record = mock_emit.call_args[0][0]
            assert not hasattr(record, 'context_str')
    
    def test_emit_with_existing_context_str(self, mock_record):
        """Should preserve existing context string."""
        with patch.object(logging.StreamHandler, 'emit') as mock_emit:
            handler = ConsoleHandlerWithContext()
            
            # Set context
            context = LoggingContext(pipeline_name="test-pipeline")
            ContextManager.set_context(context)
            
            # Add existing context string
            mock_record.context_str = "[existing:value]"
            
            # Emit log
            handler.emit(mock_record)
            
            # Verify context strings were combined
            mock_emit.assert_called_once()
            record = mock_emit.call_args[0][0]
            assert "[existing:value]" in record.context_str
            assert "[pipeline_name:test-pipeline]" in record.context_str 