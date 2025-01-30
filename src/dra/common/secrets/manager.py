"""Secrets management for DRA."""

import logging
from typing import Optional, Dict, Type, Any
from dra.common.secrets.provider import SecretsProvider

_logger = logging.getLogger(__name__)

class SecretsManager:
    """
    Central secrets management for DRA.
    
    This class manages secret providers and provides a unified interface
    for secret access across different environments.
    """
    
    _instance = None
    _provider = None
    
    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super(SecretsManager, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    def initialize(cls, provider_class: Type[SecretsProvider], **kwargs) -> None:
        """
        Initialize secrets management with specific provider.
        
        Args:
            provider_class: SecretsProvider implementation class
            **kwargs: Provider-specific initialization arguments
        """
        if cls._provider is not None:
            _logger.warning("Reinitializing secrets manager with new provider")
        cls._provider = provider_class(**kwargs)
    
    @classmethod
    def get_secret(cls, scope: str, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value.
        
        Args:
            scope: Secret scope/namespace
            key: Secret key name
            default: Default value if secret not found
            
        Returns:
            Secret value or default if not found
        
        Raises:
            RuntimeError: If secrets manager not initialized
        """
        if cls._provider is None:
            raise RuntimeError("Secrets manager not initialized")
        try:
            value = cls._provider.get_secret(scope, key)
            return value if value is not None else default
        except Exception as e:
            _logger.error("Failed to get secret %s/%s: %s", scope, key, e)
            return default
    
    @classmethod
    def list_secrets(cls, scope: str) -> Dict[str, Any]:
        """
        List available secrets in a scope.
        
        Args:
            scope: Secret scope/namespace
            
        Returns:
            Dictionary of available secrets and metadata
            
        Raises:
            RuntimeError: If secrets manager not initialized
        """
        if cls._provider is None:
            raise RuntimeError("Secrets manager not initialized")
        return cls._provider.list_secrets(scope) 