# utils/env_config.py

import os
from pathlib import Path
from typing import Dict, Any

def resolve_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve environment variables in configuration values.
    Supports ${VAR_NAME} syntax in configuration strings.
    """
    if isinstance(config, dict):
        return {key: resolve_env_vars(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [resolve_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Simple environment variable substitution
        if config.startswith("${") and config.endswith("}"):
            var_name = config[2:-1]
            return os.getenv(var_name, config)
        return config
    else:
        return config

def load_env_file(env_file_path: Path) -> None:
    """
    Load environment variables from a .env file.
    """
    if not env_file_path.exists():
        return
    
    with open(env_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()