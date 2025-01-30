# src/ara/pyspark/utils.py
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Any
from pyspark.sql import SparkSession
from dra.core.pyspark.exceptions import SparkEnvironmentError

try:
    import mssparkutils
except ImportError:
    mssparkutils = None

class SparkUtils(ABC):
    """
    Abstract base class for environment-specific Spark utilities.
    
    This class defines the interface for accessing environment-specific
    utilities and file system operations across different Spark environments.
    """
    
    @abstractmethod
    def get_utils(self) -> Any:
        """
        Get environment-specific utility instance.
        
        Returns:
            Any: Environment-specific utility object
        """
            
    @abstractmethod
    def fs_utils(self) -> Any:
        """
        Get file system utilities for the environment.
        
        Returns:
            Any: Environment-specific file system utilities
        """
        
class DatabricksUtils(SparkUtils):
    """
    Databricks-specific implementation of SparkUtils.
    
    This class provides access to Databricks utilities (DBUtils) and implements
    secret management with caching capabilities.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DatabricksUtils.
        
        Args:
            spark (SparkSession): Active Spark session
        """
        self._spark = spark
        self._dbutils = None
        self._all_secrets: Dict[Tuple[str, str], str] = {}

    def get_utils(self):
        """
        Get DBUtils with wrapped secret management.
        
        Returns:
            DBUtils: Databricks utilities with cached secret management
            
        Raises:
            SparkEnvironmentError: If DBUtils initialization fails
        """
        if self._dbutils is None:
            try:
                # First try to get DBUtils directly if we're in Databricks
                from pyspark.dbutils import DBUtils
                original_dbutils = DBUtils(self._spark)
                
                # Wrap the secrets.get method for caching
                wrapped_secrets_get = original_dbutils.secrets.get
                
                def get_secrets_wrapper(scope: str, key: str) -> str:
                    """
                    Wrapper for DBUtils secrets.get with caching.
                    
                    Args:
                        scope (str): Secret scope name
                        key (str): Secret key name
                        
                    Returns:
                        str: Secret value
                    """
                    k = (scope, key)
                    cached = self._all_secrets.get(k, None)
                    if cached is not None:
                        return cached
                    
                    v = wrapped_secrets_get(scope, key)
                    self._all_secrets[k] = v
                    return v
                
                # Create new DBUtils instance with wrapped secrets
                self._dbutils = DBUtils(self._spark)
                self._dbutils.secrets.get = get_secrets_wrapper
                
            except Exception as e:
                raise SparkEnvironmentError(f"Failed to initialize DBUtils: {str(e)}") from e
        
        return self._dbutils

    def fs_utils(self):
        """
        Get Databricks file system utilities.
        
        Returns:
            Optional[Any]: Databricks file system utilities if available, None otherwise
        """
        utils = self.get_utils()
        return utils.fs if utils else None
    
    def redact_string(self, s: str) -> str:
        """
        Redact any secret values from a string.
        
        Args:
            s (str): String to redact
            
        Returns:
            str: String with all secret values replaced with '***'
        """
        if not s:
            return s
        
        s = str(s)
        for _, v in self._all_secrets.items():
            s = s.replace(v, "***")
        
        return s

class FabricUtils(SparkUtils):
    """
    Microsoft Fabric-specific implementation of SparkUtils.
    
    This class provides access to Microsoft Fabric utilities (mssparkutils).
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize FabricUtils.
        
        Args:
            spark (SparkSession): Active Spark session
        """
        self._spark = spark
        self._utils = None

    def get_utils(self):
        """
        Get Microsoft Fabric utilities.
        
        Returns:
            Any: Microsoft Fabric utilities
            
        Raises:
            SparkEnvironmentError: If Fabric utilities are not available
        """
        if self._utils is None:
            if mssparkutils is None:
                raise SparkEnvironmentError("MS Fabric utilities not available")
            self._utils = mssparkutils
        return self._utils

    def fs_utils(self):
        """
        Get Microsoft Fabric file system utilities.
        
        Returns:
            Optional[Any]: Fabric file system utilities if available, None otherwise
        """
        utils = self.get_utils()
        return utils.fs if utils else None

class LocalUtils(SparkUtils):
    """
    Local environment implementation of SparkUtils.
    
    This class provides a minimal implementation for local development
    environments where specialized utilities are not available.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize LocalUtils.
        
        Args:
            spark (SparkSession): Active Spark session
        """
        self._spark = spark

    def get_utils(self):
        """
        Get local environment utilities.
        
        Returns:
            None: Local environment does not provide specialized utilities
        """
        return None

    def fs_utils(self):
        """
        Get local file system utilities.
        
        Returns:
            None: Local environment does not provide specialized file system utilities
        """
        return None