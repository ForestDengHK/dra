"""Exception classes for PySpark operations."""

class SparkException(Exception):
    """Base exception for Spark related errors."""
   
class SparkInitializationError(SparkException):
    """Raised when Spark session initialization fails."""
    
class SparkConfigurationError(SparkException):
    """Raised when configuration is invalid"""
    
class SparkEnvironmentError(SparkException):
    """Raised when environment detection/setup fails."""
    