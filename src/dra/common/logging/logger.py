"""DRA logging implementation.

This module provides a simple logging wrapper for DRA applications with support for
contextual logging and Azure Application Insights integration.
"""

import logging
from typing import Optional, Callable, Any
import yaml
from dra.common.secrets.manager import SecretsManager

# Module-level logger for internal use
_logger = logging.getLogger(__name__)  # pylint: disable=no-member

class DRALogger:
    """A simple logging wrapper for DRA applications.
    
    This class provides a wrapper around Python's standard logging with added
    support for contextual logging and Azure Application Insights integration.
    
    Example:
        >>> logger = DRALogger.get_logger(__name__)
        >>> logger.info("Processing", context={"pipeline": "my_pipeline"})
    """
    
    @staticmethod
    def setup(config_path: Optional[str] = None, azure_key_name: str = 'kv-instrumentation-key') -> None:
        """Initialize logging configuration.
        
        Args:
            config_path: Path to a YAML logging configuration file. If not provided,
                default configuration will be used.
            azure_key_name: Name of the Azure instrumentation key in secrets.
                Defaults to 'kv-instrumentation-key'.
        """
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s [%(levelname)s] %(name)s %(context)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'standard'
                }
            },
            'loggers': {
                'dra': {
                    'handlers': ['console'],
                    'level': 'INFO'
                }
            }
        }
        
        # Load custom configuration if provided
        if config_path:
            try:
                with open(config_path, encoding='utf-8') as f:
                    custom_config = yaml.safe_load(f)
                    if custom_config:
                        config.update(custom_config)
            except FileNotFoundError:
                _logger.warning("Logging config file not found: %s, using default config", config_path)
            except Exception as e:  # pylint: disable=broad-except
                _logger.warning(f"Failed to load config from {config_path}: {str(e)}, using default config")

        # Configure Azure Application Insights if key is available
        try:
            azure_key = SecretsManager.get_secret('azure', azure_key_name)
            if azure_key:
                config['handlers']['azure'] = {
                    'class': 'opencensus.ext.azure.log_exporter.AzureLogHandler',
                    'instrumentation_key': azure_key
                }
                config['loggers']['dra']['handlers'].append('azure')
                _logger.info("Azure Application Insights logging configured")
        except Exception as e:  # pylint: disable=broad-except
            _logger.warning("Failed to configure Azure logging: %s", e)
        
        logging.config.dictConfig(config)  # pylint: disable=no-member
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger instance with context support.
        
        Args:
            name: The name for the logger, typically __name__.
            
        Returns:
            A Logger instance with context-aware logging methods.
        """
        logger = logging.getLogger(name)
        
        def wrap_log(func: Callable) -> Callable:
            """Wrap logging function with context support."""
            def wrapped(msg: Any, *args: Any, context: Optional[dict] = None, **kwargs: Any) -> Any:
                """Add context to log message if provided."""
                if context:
                    kwargs['extra'] = kwargs.get('extra', {})
                    kwargs['extra']['context'] = ' '.join(f'[{k}:{v}]' for k, v in context.items())
                return func(msg, *args, **kwargs)
            return wrapped

        # Replace logging methods with wrapped versions
        logger.debug = wrap_log(logger.debug)
        logger.info = wrap_log(logger.info)
        logger.warning = wrap_log(logger.warning)
        logger.error = wrap_log(logger.error)
        logger.critical = wrap_log(logger.critical)
        
        return logger 