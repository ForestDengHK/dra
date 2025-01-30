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
    def initialize_session(self) -> SparkSession:
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
        
        Returns:
            SparkSession: The initialized Databricks Spark session
            
        Raises:
            SparkInitializationError: If session initialization fails
        """
        try:
            self._session = SparkSession.getActiveSession()
            if not self._session:
                # If no active session, create new one with standard configurations
                self._session = self._create_new_session()
            return self._session
        except Exception as e:
            raise SparkInitializationError(f"Failed to initialize Databricks session: {str(e)}") from e

    def get_utils(self) -> SparkUtils:
        """
        Get Databricks-specific utilities.
        
        Returns:
            SparkUtils: DatabricksUtils instance if not in unittest mode, None otherwise
        """
        if not self._utils and not os.environ.get('IS_UNITTEST'):
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

    def _create_new_session(self) -> SparkSession:
        """
        Create a new Spark session with Databricks-specific configurations.
        
        Returns:
            SparkSession: The newly created Spark session
        """
        return SparkSession.builder \
            .config("spark.jars.packages", self.get_spark_jars_packages()) \
            .config("spark.databricks.service.client.session.cache.size", 200) \
            .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED') \
            .config('spark.sql.shuffle.partitions', 1) \
            .config("spark.databricks.io.cache.enabled", 'true') \
            .config("spark.databricks.io.cache.compression.enable", 'true') \
            .config("spark.databricks.io.cache.maxMetaDataCache", "1g") \
            .getOrCreate()

    @classmethod
    def get_spark_jars_packages(cls) -> str:
        """
        Get the list of required JAR packages.
        
        Returns:
            str: Comma-separated list of JAR package coordinates
        """
        return ",".join([
            "com.microsoft.sqlserver:mssql-jdbc:8.2.1.jre8",
            "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18",
            "com.microsoft.azure:spark-mssql-connector_2.12:1.3.0-BETA",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1",
            "org.mariadb.jdbc:mariadb-java-client:3.0.8",
            "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18"
        ])

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
        
        Returns:
            SparkSession: The initialized Fabric Spark session
            
        Raises:
            SparkInitializationError: If session initialization fails
        """
        try:
            self._session = SparkSession.getActiveSession()
            if not self._session:
                self._session = self._create_new_session()
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

    def _create_new_session(self) -> SparkSession:
        """
        Create a new Spark session with Fabric-specific configurations.
        
        Returns:
            SparkSession: The newly created Spark session
        """
        return SparkSession.builder \
            .config("spark.jars.packages", self.get_spark_jars_packages()) \
            .getOrCreate()

    def get_spark_jars_packages(self) -> str:
        """
        Get the list of required JAR packages.
        
        Returns:
            str: Comma-separated list of JAR package coordinates
        """
        return DatabricksEnvironment.get_spark_jars_packages()

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
        
        Returns:
            SparkSession: The initialized local Spark session
            
        Raises:
            SparkInitializationError: If session initialization fails
        """
        try:
            self._session = SparkSession.getActiveSession()
            if not self._session:
                self._session = self._create_new_session()
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

    def _create_new_session(self) -> SparkSession:
        """
        Create a new Spark session with local environment configurations.
        
        Returns:
            SparkSession: The newly created Spark session
        """
        return SparkSession.builder \
            .master("local[*]") \
            .appName("local-spark") \
            .config("spark.jars.packages", self.get_spark_jars_packages()) \
            .getOrCreate()

    def get_spark_jars_packages(self) -> str:
        """
        Get the list of required JAR packages.
        
        Returns:
            str: Comma-separated list of JAR package coordinates
        """
        return DatabricksEnvironment.get_spark_jars_packages()