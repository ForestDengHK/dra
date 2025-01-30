"""Custom logging handlers for DRA."""

# Standard library imports first
import logging
import threading
from typing import Optional
from logging import LogRecord  # pylint: disable=no-name-in-module

# Third-party imports
from opencensus.ext.azure.log_exporter import AzureLogHandler 
from opencensus.common.schedule import QueueEvent 

# First-party/local imports
from dra.common.logging.context import ContextManager 

class AzureLogHandlerWithContext(AzureLogHandler):
    """
    Extended Azure Log Handler with context support.
    
    This handler enriches log records with context information before
    sending them to Azure Application Insights.
    """
    
    def __init__(self, connection_string: Optional[str] = None,
                 instrumentation_key: Optional[str] = None):
        """Initialize the handler with Azure credentials."""
        super().__init__(connection_string=connection_string,
                        instrumentation_key=instrumentation_key)
        self._lock = threading.RLock()
    
    def emit(self, record: LogRecord) -> None:
        """
        Emit a log record to Azure Application Insights.
        
        Enriches the log record with context information before sending.
        
        Args:
            record: The log record to emit
        """
        context = ContextManager.get_context()
        if context:
            if not hasattr(record, 'custom_dimensions'):
                record.custom_dimensions = {}
            record.custom_dimensions.update(context.as_dict())
        
        with self._lock:
            super().emit(record)
    
    def flush(self, timeout: int = 10) -> None:
        """
        Ensure all logs are sent to Azure.
        
        Args:
            timeout: Maximum time to wait for flush completion (seconds)
        """
        with self._lock:
            try:
                flush_event = QueueEvent('flush-event')
                self._queue.put(flush_event)
                super().flush()
                flush_event.wait(timeout)
            except Exception as e:
                logging.error("Failed to flush Azure Log Handler: %s", e) 
                
class ConsoleHandlerWithContext(logging.StreamHandler):
    """Console handler that includes context information in log records."""
    
    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record with context information."""
        context = ContextManager.get_context()
        if context:
            if not hasattr(record, 'context_str'):
                context_dict = context.as_dict()
                record.context_str = ' '.join(f'[{k}:{v}]' 
                                           for k, v in context_dict.items())
        super().emit(record) 