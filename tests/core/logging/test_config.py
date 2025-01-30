"""Unit tests for logging configuration."""

from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
import yaml

from dra.common.logging.config import (
    LoggingConfig,
    LocalConfigSource,
    DBFSConfigSource,
    AzureHandlerConfig
)
from dra.common.secrets.manager import SecretsManager

@pytest.fixture
def temp_config_file(tmp_path):
    """Create a temporary config file for testing."""
    config = {
        'version': 1,
        'handlers': {
            'test': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG'
            }
        }
    }
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.dump(config, f)
    return config_file

@pytest.fixture
def mock_secrets_manager():
    """Create a mock secrets manager."""
    mock = Mock(spec=SecretsManager)
    mock.get_secret.return_value = "test-key"
    return mock

@pytest.fixture
def mock_spark_session():
    """Create a mock Spark session."""
    mock = MagicMock()
    mock.table = MagicMock()
    return mock

@pytest.fixture
def mock_dbutils():
    """Create a mock DBUtils."""
    mock = MagicMock()
    mock.fs.head = MagicMock(return_value="version: 1\nhandlers:\n  test:\n    class: logging.StreamHandler")
    return mock

class TestLocalConfigSource:
    """Test cases for LocalConfigSource."""
    
    def test_load_existing_file(self):
        """Should successfully load existing config file."""
        source = LocalConfigSource()
        config = source.load(temp_config_file)
        assert config is not None
        assert config['version'] == 1
        assert 'handlers' in config
    
    def test_load_nonexistent_file(self):
        """Should return None for non-existent file."""
        source = LocalConfigSource()
        config = source.load(Path("nonexistent.yaml"))
        assert config is None
    
    def test_load_invalid_yaml(self, tmp_path):
        """Should handle invalid YAML gracefully."""
        invalid_file = tmp_path / "invalid.yaml"
        with open(invalid_file, 'w', encoding='utf-8') as f:
            f.write("invalid: yaml: content:")
        
        source = LocalConfigSource()
        config = source.load(invalid_file)
        assert config is None

class TestDBFSConfigSource:
    """Test cases for DBFSConfigSource."""
    
    @patch('dra.common.logging.config.SparkSessionManager')
    def test_load_dbfs_file(self, mock_manager):
        """Should load config from DBFS."""
        # Setup mock
        mock_manager.return_value.session = mock_spark_session
        mock_manager.return_value.utils.get_utils.return_value = mock_dbutils
        
        source = DBFSConfigSource()
        config = source.load("dbfs:/test/config.yaml")
        
        assert config is not None
        assert config['version'] == 1
        assert 'handlers' in config
        mock_dbutils.fs.head.assert_called_once_with("dbfs:/test/config.yaml")
    
    @patch('dra.common.logging.config.SparkSessionManager')
    def test_load_no_spark_utils(self, mock_manager):
        """Should handle missing Spark utils gracefully."""
        mock_manager.return_value.utils = None
        
        source = DBFSConfigSource()
        config = source.load("dbfs:/test/config.yaml")
        
        assert config is None
    
    @patch('dra.common.logging.config.SparkSessionManager')
    def test_load_dbfs_error(self, mock_manager):
        """Should handle DBFS errors gracefully."""
        mock_manager.return_value.session = mock_spark_session
        mock_manager.return_value.utils.get_utils.return_value = mock_dbutils
        mock_dbutils.fs.head.side_effect = Exception("DBFS error")
        
        source = DBFSConfigSource()
        config = source.load("dbfs:/test/config.yaml")
        
        assert config is None

class TestAzureHandlerConfig:
    """Test cases for AzureHandlerConfig."""
    
    def test_configure_with_key(self):
        """Should configure Azure handler when key is available."""
        handler = AzureHandlerConfig(mock_secrets_manager)
        config = {'handlers': {}, 'loggers': {'dra': {'handlers': []}}}
        
        handler.configure(config)
        
        assert 'azure' in config['handlers']
        assert config['handlers']['azure']['instrumentation_key'] == "test-key"
        assert 'azure' in config['loggers']['dra']['handlers']
    
    def test_configure_no_key(self):
        """Should skip Azure configuration when key is not available."""
        mock_secrets_manager.get_secret.return_value = None
        handler = AzureHandlerConfig(mock_secrets_manager)
        config = {'handlers': {}, 'loggers': {'dra': {'handlers': []}}}
        
        handler.configure(config)
        
        assert 'azure' not in config['handlers']
        assert 'azure' not in config['loggers']['dra']['handlers']

class TestLoggingConfig:
    """Test cases for LoggingConfig."""
    
    def test_default_config(self):
        """Should provide valid default configuration."""
        config = LoggingConfig()
        result = config.load_config()
        
        assert result['version'] == 1
        assert 'formatters' in result
        assert 'handlers' in result
        assert 'loggers' in result
        assert 'dra' in result['loggers']
    
    def test_merge_configs(self):
        """Should correctly merge configurations."""
        base = {'a': 1, 'b': {'c': 2}}
        override = {'b': {'d': 3}, 'e': 4}
        
        LoggingConfig._merge_configs(base, override)
        
        assert base == {'a': 1, 'b': {'c': 2, 'd': 3}, 'e': 4}
    
    def test_load_config_with_file(self):
        """Should load and merge file configuration."""
        config = LoggingConfig()
        result = config.load_config(temp_config_file)
        
        assert result['version'] == 1
        assert 'test' in result['handlers']
        assert 'console' in result['handlers']  # From default config
    
    def test_load_config_with_invalid_file(self):
        """Should handle invalid config file gracefully."""
        config = LoggingConfig()
        result = config.load_config("nonexistent.yaml")
        
        assert result['version'] == 1  # Should return default config
        assert 'console' in result['handlers']
    
    def test_load_config_with_dbfs_path(self):
        """Should handle DBFS paths."""
        config = LoggingConfig()
        with patch('dra.common.logging.config.DBFSConfigSource') as mock_source:
            mock_source.return_value.load.return_value = {'version': 2}
            result = config.load_config("dbfs:/test/config.yaml")
            
            assert result['version'] == 2
            mock_source.return_value.load.assert_called_once_with("dbfs:/test/config.yaml") 