# src/ara/pyspark/session.py
from typing import Optional
from pyspark.sql import SparkSession, SQLContext
import logging
from py4j.protocol import Py4JError
from pyspark.sql.utils import AnalysisException
from dra.core.pyspark.detector import EnvironmentDetector
from dra.core.pyspark.environment import SparkEnvironment
from dra.core.pyspark.exceptions import SparkInitializationError
from dra.core.pyspark.utils import SparkUtils

# Configure logging
logger = logging.getLogger(__name__)

class SparkSessionManager:
    """
    Singleton manager class for Spark session and related contexts.
    
    This class manages the lifecycle of a Spark session and its associated contexts
    (SparkContext, SQLContext) in a singleton pattern. It automatically detects
    the appropriate environment and initializes the session accordingly.
    """
    
    _instance = None
    
    def __init__(self):
        """Initialize instance attributes."""
        self._environment = None
        self._session = None
        self._utils = None
        self._sc = None
        self._sql_context = None
    
    def __new__(cls):
        """
        Create or return the singleton instance of SparkSessionManager.
        
        Returns:
            SparkSessionManager: The singleton instance
        """
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
            cls._instance.__init__()
            cls._instance._initialize()
        return cls._instance

    def _initialize_contexts(self):
        """
        Initialize SparkContext and SQLContext with proper error handling.
        Both contexts will be set to None if initialization fails.
        """
        try:
            self._sc = self._session.sparkContext
            logger.info("Successfully initialized SparkContext")
        except (Py4JError, AttributeError) as e:
            self._sc = None
            logger.warning("Failed to initialize SparkContext: %s. This may be expected in serverless or shared cluster environments.", str(e))

        try:
            if self._sc is not None:
                self._sql_context = SQLContext(self._sc, self._session)
                logger.info("Successfully initialized SQLContext")
            else:
                self._sql_context = None
                logger.info("Skipping SQLContext initialization as SparkContext is not available")
        except (Py4JError, AnalysisException) as e:
            self._sql_context = None
            logger.warning("Failed to initialize SQLContext: %s. This may be expected in serverless or shared cluster environments.", str(e))

    def _initialize(self):
        """
        Initialize the Spark session and related contexts.
        
        This method:
        1. Detects the appropriate Spark environment
        2. Initializes the Spark session
        3. Sets up environment-specific utilities
        4. Attempts to create SparkContext and SQLContext if supported
        
        Raises:
            SparkInitializationError: If session initialization fails
        """
        try:
            self._environment: SparkEnvironment = EnvironmentDetector.detect()
            self._session = self._environment.initialize_session()
            self._utils = self._environment.get_utils()
            
            # Initialize additional contexts with proper error handling
            self._initialize_contexts()
            
        except Exception as e:
            raise SparkInitializationError("Failed to initialize Spark session: %s" % str(e)) from e

    @property
    def session(self) -> SparkSession:
        """
        Get the current Spark session.
        
        Returns:
            SparkSession: The active Spark session
        """
        return self._session

    @property
    def utils(self) -> Optional[SparkUtils]:
        """
        Get environment-specific utilities.
        
        Returns:
            Optional[SparkUtils]: Environment-specific utilities if available, None otherwise
        """
        return self._utils

    @property
    def sc(self):
        """
        Get the SparkContext.
        
        Returns:
            SparkContext: The active SparkContext
        """
        return self._sc

    @property
    def sql_context(self) -> SQLContext:
        """
        Get the SQLContext.
        
        Returns:
            SQLContext: The active SQLContext
        """
        return self._sql_context

    @property
    def environment(self) -> str:
        """
        Get the current environment name.
        
        Returns:
            str: The name of the current environment (e.g., 'local', 'databricks', 'fabric')
        """
        return self._environment.environment_name