# src/ara/pyspark/environment.py
from abc import ABC, abstractmethod
import os

from pyspark.sql import SparkSession

from dra.core.pyspark.exceptions import SparkInitializationError
from dra.core.pyspark.utils import SparkUtils, DatabricksUtils, FabricUtils, LocalUtils


class SparkEnvironment(ABC):
    """
    Abstract base class defining the interface for Spark environments.
    
    This class provides a common interface for different Spark environments
    (Local, Databricks, Fabric) to implement their specific initialization
    and utility access patterns.
    """
    
    @abstractmethod
    def initialize_session(self):
        """
        Initialize and return a Spark session for the environment.
        
        Returns:
            SparkSession: The initialized Spark session
        Raises:
            SparkInitializationError: If session initialization fails
        """
        
    
    @abstractmethod
    def get_utils(self) -> SparkUtils:
        """
        Get environment-specific utilities.
        
        Returns:
            SparkUtils: Environment-specific utility instance
        """
        
    
    @property
    @abstractmethod
    def environment_name(self) -> str:
        """
        Get the name of the environment.
        
        Returns:
            str: The environment name (e.g., 'local', 'databricks', 'fabric')
        """

class DatabricksEnvironment(SparkEnvironment):
    """
    Implementation of SparkEnvironment for Databricks.
    
    This class handles Databricks-specific session initialization and utilities,
    including configuration of Databricks-specific Spark properties.
    """
    
    def __init__(self):
        """Initialize a new DatabricksEnvironment instance."""
        self._session = None
        self._utils = None

    def initialize_session(self) -> SparkSession:
        """
        Initialize a Spark session configured for Databricks environment.
        """
        try:
            self._session = SparkSession.getActiveSession()
            if not self._session:
                from databricks.connect import DatabricksSession
                self._session = DatabricksSession.builder.getOrCreate()
            
            # Check for serverless/shared cluster environment
            if os.environ.get("IS_SERVERLESS", False):
                self._is_serverless = True
            else:
                self._is_shared_cluster = self._session.conf.get(
                    "spark.databricks.clusterUsageTags.clusterUnityCatalogMode", 
                    None
                ) == "USER_ISOLATION"
                
            return self._session
        except Exception as e:
            raise SparkInitializationError(f"Failed to initialize Databricks session: {str(e)}") from e

    @property
    def is_serverless(self) -> bool:
        """Check if running in serverless mode."""
        return getattr(self, '_is_serverless', False)

    @property 
    def is_shared_cluster(self) -> bool:
        """Check if running in shared cluster."""
        return getattr(self, '_is_shared_cluster', False)

    def get_utils(self) -> SparkUtils:
        """
        Get Databricks-specific utilities.
        
        Returns:
            SparkUtils: DatabricksUtils instance
        """
        if not self._utils:
            self._utils = DatabricksUtils(self._session)
        return self._utils

    @property
    def environment_name(self) -> str:
        """
        Get the environment name.
        
        Returns:
            str: Always returns 'databricks'
        """
        return "databricks"

class FabricEnvironment(SparkEnvironment):
    """
    Implementation of SparkEnvironment for Microsoft Fabric.
    
    This class handles Fabric-specific session initialization and utilities.
    """
    
    def __init__(self):
        """Initialize a new FabricEnvironment instance."""
        self._session = None
        self._utils = None

    def initialize_session(self) -> SparkSession:
        """
        Initialize a Spark session configured for Fabric environment.
        """
        try:
            self._session = SparkSession.getActiveSession()
            if not self._session:
                self._session = SparkSession.builder \
                    .master("fabric") \
                    .appName("fabric-spark") \
                    .getOrCreate()
            return self._session
        except Exception as e:
            raise SparkInitializationError(f"Failed to initialize Fabric session: {str(e)}") from e

    def get_utils(self) -> SparkUtils:
        """
        Get Fabric-specific utilities.
        
        Returns:
            SparkUtils: FabricUtils instance
        """
        if not self._utils:
            self._utils = FabricUtils(self._session)
        return self._utils

    @property
    def environment_name(self) -> str:
        """
        Get the environment name.
        
        Returns:
            str: Always returns 'fabric'
        """
        return "fabric"

class LocalEnvironment(SparkEnvironment):
    """
    Implementation of SparkEnvironment for local development.
    
    This class handles local development specific session initialization
    and utilities, suitable for testing and development purposes.
    """
    
    def __init__(self):
        """Initialize a new LocalEnvironment instance."""
        self._session = None
        self._utils = None

    def initialize_session(self) -> SparkSession:
        """
        Initialize a Spark session configured for local environment.
        """
        try:
            self._session = SparkSession.getActiveSession()
            if not self._session:
                self._session = SparkSession.builder \
                    .master("local[*]") \
                    .appName("local-spark") \
                    .getOrCreate()
            return self._session
        except Exception as e:
            raise SparkInitializationError(
                f"Failed to initialize local session: {str(e)}"
            ) from e

    def get_utils(self) -> SparkUtils:
        """
        Get local environment utilities.
        
        Returns:
            SparkUtils: LocalUtils instance
        """
        if not self._utils:
            self._utils = LocalUtils(self._session)
        return self._utils

    @property
    def environment_name(self) -> str:
        """
        Get the environment name.
        
        Returns:
            str: Always returns 'local'
        """
        return "local"