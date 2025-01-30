"""Example usage of DRA logging system."""

from dra.common.logging import DRALogger, LoggingContext
from datetime import datetime

# Get a logger instance
logger = DRALogger.get_logger(__name__)

def process_data():
    """Example function showing context usage."""
    # Set context for all logging in this function
    with logger.with_context(
        pipeline_name="example_pipeline",
        job_name="process_data",
        job_timestamp=datetime.now()
    ):
        logger.info("Starting data processing")
        
        # Nested context
        with logger.with_context(scope="validation"):
            logger.debug("Validating input data")
            # ... validation code ...
            logger.info("Validation complete")
        
        # Original context is restored
        logger.info("Processing complete")

def main():
    """Main example function."""
    # Initialize logging with custom config (optional)
    DRALogger.setup_logging()
    
    # Set global context
    context = LoggingContext(
        pipeline_name="example",
        job_namespace="development",
        streaming_enabled=True
    )
    logger.set_context(context)
    
    # Log with context
    logger.info("Starting example")
    process_data()
    logger.info("Example complete")

if __name__ == "__main__":
    main() 