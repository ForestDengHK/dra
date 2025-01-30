"""Configuration management for DRA logging."""

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Union

import yaml

from dra.common.secrets.manager import SecretsManager
from dra.core.pyspark.session import SparkSessionManager

_logger = logging.getLogger(__name__)

class ConfigurationSource(Protocol):
    """Protocol for configuration sources."""
    
    def load(self, path: Union[str, Path]) -> Optional[Dict[str, Any]]:
        """Load configuration from source."""

class LocalConfigSource:
    """Local filesystem configuration source."""
    
    def load(self, path: Union[str, Path]) -> Optional[Dict[str, Any]]:
        """Load configuration from local filesystem."""
        path = Path(path)
        if not path.exists():
            return None
            
        try:
            with open(path, encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            _logger.warning("Failed to load config from %s: %s", path, e)
            return None

class DBFSConfigSource:
    """Databricks filesystem configuration source."""
    
    def load(self, path: str) -> Optional[Dict[str, Any]]:
        """Load configuration from DBFS."""
        try:
            manager = SparkSessionManager()
            if not manager.utils:
                return None
                
            content = manager.utils.fs_utils().head(path)
            return yaml.safe_load(content)
        except ImportError:
            return None  # Not in Databricks environment
        except Exception as e:
            _logger.warning("Failed to load config from DBFS %s: %s", path, e)
            return None

class HandlerConfig(ABC):
    """Base class for handler configuration."""
    
    @abstractmethod
    def configure(self, config: Dict[str, Any]) -> None:
        """Configure the handler."""

class AzureHandlerConfig(HandlerConfig):
    """Azure handler configuration."""
    
    def __init__(self, secret_manager: SecretsManager):
        """Initialize with secret manager."""
        self._secret_manager = secret_manager
    
    def configure(self, config: Dict[str, Any], 
                 azure_key_name: str = 'kv-instrumentation-key') -> None:
        """Configure Azure handler."""
        try:
            azure_key = self._secret_manager.get_secret('azure', azure_key_name)
            if not azure_key:
                return
                
            config['handlers']['azure'] = {
                'class': 'dra.common.logging.handlers.AzureLogHandlerWithContext',
                'level': 'INFO',
                'instrumentation_key': azure_key
            }
            config['loggers']['dra']['handlers'].append('azure')
            _logger.info("Azure Application Insights logging configured")
        except Exception as e:
            _logger.warning("Failed to configure Azure logging: %s", e)

class LoggingConfig:
    """Manages logging configuration for DRA."""
    
    DEFAULT_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(context_str)s%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'simple': {
                'format': '[%(levelname)s] %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'dra.common.logging.handlers.ConsoleHandlerWithContext',
                'level': 'DEBUG',
                'formatter': 'standard',
                'stream': 'ext://sys.stdout'
            }
        },
        'loggers': {
            'dra': {
                'level': 'DEBUG',
                'handlers': ['console'],
                'propagate': False
            }
        }
    }
    
    def __init__(self):
        """Initialize configuration sources."""
        self._sources = {
            'local': LocalConfigSource(),
            'dbfs': DBFSConfigSource()
        }
        self._handlers = {
            'azure': AzureHandlerConfig(SecretsManager())
        }
    
    def load_config(self, config_path: Optional[Union[str, Path]] = None,
                   azure_key_name: str = 'kv-instrumentation-key') -> Dict[str, Any]:
        """Load logging configuration."""
        config = self.DEFAULT_CONFIG.copy()
        
        if config_path:
            file_config = self._load_from_source(config_path)
            if file_config:
                self._merge_configs(config, file_config)
        
        # Configure handlers
        self._handlers['azure'].configure(config, azure_key_name)
        
        return config
    
    def _load_from_source(self, path: Union[str, Path]) -> Optional[Dict[str, Any]]:
        """Load configuration from appropriate source."""
        try:
            if isinstance(path, str) and path.startswith('dbfs:'):
                return self._sources['dbfs'].load(path)
            return self._sources['local'].load(path)
        except Exception as e:
            _logger.warning(str(e))
            return None
    
    @staticmethod
    def _merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> None:
        """Recursively merge configuration dictionaries."""
        for key, value in override.items():
            if isinstance(value, dict) and key in base:
                LoggingConfig._merge_configs(base[key], value)
            else:
                base[key] = value 