"""Unit tests for logging context management."""

from datetime import datetime

from dra.common.logging.context import LoggingContext, ContextManager

class TestLoggingContext:
    """Test cases for LoggingContext."""
    
    def test_default_values(self):
        """Should initialize with default values."""
        context = LoggingContext()
        
        assert context.pipeline_name is None
        assert context.job_name is None
        assert context.job_namespace is None
        assert context.job_timestamp is None
        assert context.operation_id is None
        assert context.scope == ""
        assert context.core_engine == "pyspark"
        assert context.streaming_enabled is False
    
    def test_custom_values(self):
        """Should initialize with custom values."""
        timestamp = datetime.now()
        context = LoggingContext(
            pipeline_name="test-pipeline",
            job_name="test-job",
            job_namespace="test",
            job_timestamp=timestamp,
            operation_id="123",
            scope="test-scope",
            core_engine="spark",
            streaming_enabled=True
        )
        
        assert context.pipeline_name == "test-pipeline"
        assert context.job_name == "test-job"
        assert context.job_namespace == "test"
        assert context.job_timestamp == timestamp
        assert context.operation_id == "123"
        assert context.scope == "test-scope"
        assert context.core_engine == "spark"
        assert context.streaming_enabled is True
    
    def test_as_dict(self):
        """Should convert to dictionary correctly."""
        timestamp = datetime.now()
        context = LoggingContext(
            pipeline_name="test-pipeline",
            job_timestamp=timestamp
        )
        
        result = context.as_dict()
        
        assert result['pipeline_name'] == "test-pipeline"
        assert result['job_timestamp'] == str(timestamp)
        assert 'job_name' not in result  # None values should be excluded

class TestContextManager:
    """Test cases for ContextManager."""
    
    def test_get_set_context(self):
        """Should get and set context correctly."""
        context = LoggingContext(pipeline_name="test")
        
        ContextManager.set_context(context)
        result = ContextManager.get_context()
        
        assert result is context
        assert result.pipeline_name == "test"
    
    def test_context_manager(self):
        """Should manage context stack correctly."""
        outer_context = LoggingContext(pipeline_name="outer")
        inner_context = LoggingContext(pipeline_name="inner")
        
        ContextManager.set_context(outer_context)
        
        with ContextManager.context(pipeline_name="inner"):
            current = ContextManager.get_context()
            assert current.pipeline_name == "inner"
        
        # Should restore outer context
        current = ContextManager.get_context()
        assert current.pipeline_name == "outer"
    
    def test_nested_context(self):
        """Should handle nested contexts correctly."""
        with ContextManager.context(pipeline_name="outer"):
            assert ContextManager.get_context().pipeline_name == "outer"
            
            with ContextManager.context(job_name="inner"):
                current = ContextManager.get_context()
                assert current.pipeline_name == "outer"  # Inherited
                assert current.job_name == "inner"  # Added
            
            # Should restore only pipeline name
            current = ContextManager.get_context()
            assert current.pipeline_name == "outer"
            assert current.job_name is None
    
    def test_thread_isolation(self):
        """Should isolate context between threads."""
        import threading
        import queue
        
        result_queue = queue.Queue()
        
        def thread_func():
            ContextManager.set_context(LoggingContext(pipeline_name="thread"))
            result_queue.put(ContextManager.get_context().pipeline_name)
        
        # Set main thread context
        ContextManager.set_context(LoggingContext(pipeline_name="main"))
        
        # Run other thread
        thread = threading.Thread(target=thread_func)
        thread.start()
        thread.join()
        
        # Verify contexts
        assert ContextManager.get_context().pipeline_name == "main"
        assert result_queue.get() == "thread" 