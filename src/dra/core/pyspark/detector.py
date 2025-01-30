# src/ara/pyspark/detector.py
import os
from pyspark.sql import SparkSession
from dra.core.pyspark.environment import (
    SparkEnvironment,
    LocalEnvironment,
    DatabricksEnvironment,
    FabricEnvironment,
)
from dra.core.pyspark.exceptions import SparkEnvironmentError

class EnvironmentDetector:
    """
    A utility class for detecting the current Spark environment.
    
    This class provides functionality to automatically detect and return the appropriate
    Spark environment (Local, Databricks, or Fabric) based on the current runtime context.
    """
    
    @staticmethod
    def detect() -> SparkEnvironment:
        """
        Automatically detect and return the appropriate Spark environment.
        
        The detection follows this logic:
        1. If IS_UNITTEST environment variable is set, returns LocalEnvironment
        2. If active Spark session exists:
           - If databricks config found, returns DatabricksEnvironment
           - If fabric config found, returns FabricEnvironment
        3. Defaults to LocalEnvironment if no specific environment detected
        
        Returns:
            SparkEnvironment: An instance of the appropriate SparkEnvironment subclass
            
        Raises:
            SparkEnvironmentError: If environment detection fails
        """
        try:
            # Check if running in unittest mode
            if os.environ.get('IS_UNITTEST'):
                return LocalEnvironment()

            # Try to get active session first
            spark = SparkSession.getActiveSession()
            
            if spark:
                # Check for Databricks environment
                if "databricks" in spark.sparkContext.getConf().getAll():
                    return DatabricksEnvironment()
                # Check for Fabric environment
                elif "fabric" in spark.sparkContext.getConf().getAll():
                    return FabricEnvironment()
            
            # Default to Local if no specific environment detected
            return LocalEnvironment()
            
        except Exception as e:
            raise SparkEnvironmentError(f"Failed to detect environment: {str(e)}") from e