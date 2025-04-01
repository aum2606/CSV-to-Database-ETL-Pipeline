import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

class Settings:
    """Configuration settings manager for the ETL pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize settings with optional config file path.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config: Dict[str, Any] = {
            # Default settings
            "database": {
                "type": "postgresql",  # or "mysql"
                "host": "localhost",
                "port": 5432,  # PostgreSQL default
                "database": "etl_target",
                "user": "etl_user",
                "password": "",
                "connection_timeout": 30,
                "max_retries": 3,
                "retry_delay": 5,
            },
            "csv": {
                "input_dir": "./data/input",
                "archive_dir": "./data/archive",
                "error_dir": "./data/error",
                "delimiter": ",",
                "quotechar": '"',
                "encoding": "utf-8",
                "batch_size": 10000,
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file": "./logs/etl.log",
                "max_size": 10485760,  # 10MB
                "backup_count": 5,
            },
            "processing": {
                "parallel": True,
                "max_workers": 4,
                "chunk_size": 100000,
            }
        }
        
        # Load from file if provided
        if config_path:
            self._load_from_file(config_path)
        
        # Override with environment variables
        self._load_from_env()
        
        # Ensure directories exist
        self._setup_directories()
    
    def _load_from_file(self, config_path: str) -> None:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                file_config = yaml.safe_load(f)
                if file_config:
                    self._deep_update(self.config, file_config)
        except Exception as e:
            logging.warning(f"Failed to load config from {config_path}: {str(e)}")
    
    def _load_from_env(self) -> None:
        """Override configuration with environment variables."""
        # Database settings
        if os.getenv("ETL_DB_TYPE"):
            self.config["database"]["type"] = os.getenv("ETL_DB_TYPE")
        if os.getenv("ETL_DB_HOST"):
            self.config["database"]["host"] = os.getenv("ETL_DB_HOST")
        if os.getenv("ETL_DB_PORT"):
            self.config["database"]["port"] = int(os.getenv("ETL_DB_PORT", "5432"))
        if os.getenv("ETL_DB_NAME"):
            self.config["database"]["database"] = os.getenv("ETL_DB_NAME")
        if os.getenv("ETL_DB_USER"):
            self.config["database"]["user"] = os.getenv("ETL_DB_USER")
        if os.getenv("ETL_DB_PASSWORD"):
            self.config["database"]["password"] = os.getenv("ETL_DB_PASSWORD")
        
        # CSV settings
        if os.getenv("ETL_CSV_INPUT_DIR"):
            self.config["csv"]["input_dir"] = os.getenv("ETL_CSV_INPUT_DIR")
        if os.getenv("ETL_CSV_BATCH_SIZE"):
            self.config["csv"]["batch_size"] = int(os.getenv("ETL_CSV_BATCH_SIZE", "10000"))
        
        # Logging settings
        if os.getenv("ETL_LOG_LEVEL"):
            self.config["logging"]["level"] = os.getenv("ETL_LOG_LEVEL")
    
    def _deep_update(self, d: Dict[str, Any], u: Dict[str, Any]) -> None:
        """Recursively update nested dictionaries."""
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v
    
    def _setup_directories(self) -> None:
        """Ensure all required directories exist."""
        directories = [
            self.config["csv"]["input_dir"],
            self.config["csv"]["archive_dir"],
            self.config["csv"]["error_dir"],
            os.path.dirname(self.config["logging"]["file"]),
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def get(self, section: str, key: Optional[str] = None) -> Any:
        """
        Get configuration value.
        
        Args:
            section: Configuration section
            key: Optional key within section
            
        Returns:
            Configuration value
        """
        if section not in self.config:
            raise KeyError(f"Configuration section '{section}' not found")
        
        if key is None:
            return self.config[section]
            
        if key not in self.config[section]:
            raise KeyError(f"Configuration key '{key}' not found in section '{section}'")
            
        return self.config[section][key]