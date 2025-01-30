"""Logging context management for DRA."""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from datetime import datetime
import threading
from contextlib import contextmanager

@dataclass
class LoggingContext:
    """
    Holds contextual information for logging.
    
    This class maintains context information that will be included in log entries,
    such as pipeline name, job details, and operation IDs.
    """
    pipeline_name: Optional[str] = None
    job_name: Optional[str] = None
    job_namespace: Optional[str] = None
    job_timestamp: Optional[datetime] = None
    operation_id: Optional[str] = None
    scope: str = ""
    core_engine: str = "pyspark"
    streaming_enabled: bool = False
    
    def as_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        data = asdict(self)
        if self.job_timestamp:
            data['job_timestamp'] = str(self.job_timestamp)
        return {k: str(v) for k, v in data.items() if v is not None}

class ContextManager:
    """Thread-safe context manager for logging contexts."""
    
    _context = threading.local()
    
    @classmethod
    def get_context(cls) -> Optional[LoggingContext]:
        """Get the current thread's logging context."""
        return getattr(cls._context, 'current', None)
    
    @classmethod
    def set_context(cls, context: LoggingContext) -> None:
        """Set the current thread's logging context."""
        cls._context.current = context
    
    @classmethod
    @contextmanager
    def context(cls, **kwargs):
        """
        Context manager for temporarily modifying logging context.
        
        Usage:
            with ContextManager.context(pipeline_name="my_pipeline"):
                logger.info("Processing")
        """
        old_context = cls.get_context()
        new_context = LoggingContext(**kwargs) if old_context is None else \
                     LoggingContext(**{**asdict(old_context), **kwargs})
        cls.set_context(new_context)
        try:
            yield new_context
        finally:
            if old_context is None:
                cls._context.current = None
            else:
                cls.set_context(old_context) 