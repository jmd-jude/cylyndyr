import os
import yaml
from typing import Dict, Any, Optional

class SchemaManager:
    """Manages database schema configurations and user customizations."""
    
    def __init__(self, config_dir: str = "schema_configs"):
        """Initialize schema manager with config directory."""
        self.config_dir = config_dir
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

    def get_config_path(self, db_type: str) -> str:
        """Get path to schema config file for given database type."""
        return os.path.join(self.config_dir, f"{db_type}_schema_config.yaml")

    def load_config(self, db_type: str) -> Optional[Dict[str, Any]]:
        """Load existing schema configuration if it exists."""
        config_path = self.get_config_path(db_type)
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return None

    def save_config(self, db_type: str, config: Dict[str, Any]) -> None:
        """Save schema configuration to file."""
        config_path = self.get_config_path(db_type)
        with open(config_path, 'w') as f:
            yaml.dump(config, f, sort_keys=False, default_flow_style=False)

    def update_field_description(self, db_type: str, table: str, field: str, description: str) -> None:
        """Update description for a specific field in the schema."""
        config = self.load_config(db_type)
        if config and table in config['tables'] and field in config['tables'][table]['fields']:
            config['tables'][table]['fields'][field]['description'] = description
            self.save_config(db_type, config)

    def update_table_description(self, db_type: str, table: str, description: str) -> None:
        """Update description for a specific table in the schema."""
        config = self.load_config(db_type)
        if config and table in config['tables']:
            config['tables'][table]['description'] = description
            self.save_config(db_type, config)

    def update_business_context(self, db_type: str, description: str, key_concepts: list) -> None:
        """Update business context in the schema configuration."""
        config = self.load_config(db_type)
        if config:
            if 'business_context' not in config:
                config['business_context'] = {}
            config['business_context']['description'] = description
            config['business_context']['key_concepts'] = key_concepts
            self.save_config(db_type, config)

    def get_tables(self, db_type: str) -> list:
        """Get list of tables from schema configuration."""
        config = self.load_config(db_type)
        return list(config['tables'].keys()) if config else []

    def get_fields(self, db_type: str, table: str) -> list:
        """Get list of fields for a specific table."""
        config = self.load_config(db_type)
        if config and table in config['tables']:
            return list(config['tables'][table]['fields'].keys())
        return []

    def get_field_info(self, db_type: str, table: str, field: str) -> Dict[str, Any]:
        """Get detailed information about a specific field."""
        config = self.load_config(db_type)
        if config and table in config['tables'] and field in config['tables'][table]['fields']:
            return config['tables'][table]['fields'][field]
        return {}
