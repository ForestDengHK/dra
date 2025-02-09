"""Secret management providers for DRA."""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class SecretsProvider(ABC):
    """
    Abstract base class for secret management providers.
    
    This interface allows different secret management implementations
    (Databricks, Azure KeyVault, HashiCorp Vault, etc.)
    """
    
    @abstractmethod
    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """
        Retrieve a secret value.
        
        Args:
            scope: Secret scope/namespace
            key: Secret key name
            
        Returns:
            Secret value if found, None otherwise
        """
        
    
    @abstractmethod
    def list_secrets(self, scope: str) -> Dict[str, Any]:
        """
        List all accessible secrets in a scope.
        
        Args:
            scope: Secret scope/namespace
            
        Returns:
            Dictionary of secret keys and metadata (not values)
        """
        
    
    @abstractmethod
    def validate_access(self, scope: str) -> bool:
        """
        Validate access to a secret scope.
        
        Args:
            scope: Secret scope/namespace
            
        Returns:
            True if scope is accessible, False otherwise
        """
         