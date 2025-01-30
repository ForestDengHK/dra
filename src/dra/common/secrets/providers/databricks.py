"""Databricks secrets provider implementation."""

from typing import Optional, Dict, Any
from dra.common.secrets.provider import SecretsProvider
from dra.core.pyspark.session import SparkSessionManager

class DatabricksSecretsProvider(SecretsProvider):
    """Secrets provider implementation for Databricks."""
    
    def __init__(self):
        """Initialize Databricks secrets provider."""
        self._manager = SparkSessionManager()
        if not self._manager.utils:
            raise RuntimeError("Not in Databricks environment")
    
    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """Get secret from Databricks secrets."""
        try:
            return self._manager.utils.get_utils().secrets.get(scope, key)
        except Exception:
            return None
    
    def list_secrets(self, scope: str) -> Dict[str, Any]:
        """List secrets in Databricks scope."""
        try:
            # Note: Actual implementation needed
            # Databricks doesn't provide direct API for listing secrets
            return {}
        except Exception:
            return {}
    
    def validate_access(self, scope: str) -> bool:
        """Validate access to Databricks secret scope."""
        try:
            # Try to list the scope
            self._manager.utils.get_utils().secrets.list(scope)
            return True
        except Exception:
            return False 