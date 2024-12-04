import os
import yaml
from typing import Dict, Any, Optional, List
from src.database.db_manager import DatabaseManager

class SchemaManager:
    """Manages database schema configurations and user customizations."""
    
    def __init__(self, config_dir: str = "schema_configs"):
        """Initialize schema manager with config directory and database manager."""
        self.config_dir = config_dir
        self.db_manager = DatabaseManager()
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

    def get_config_path(self, connection_id: str) -> str:
        """Get path to schema config file for given connection."""
        return os.path.join(self.config_dir, f"{connection_id}_schema_config.yaml")

    def load_config(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Load schema configuration for a connection."""
        schema_config = self.db_manager.get_schema_config(connection_id)
        if schema_config:
            return schema_config['config']
        
        # Fall back to file-based config if exists (for backward compatibility)
        config_path = self.get_config_path(connection_id)
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return None

    def save_config(self, connection_id: str, config: Dict[str, Any]) -> None:
        """Save schema configuration for a connection."""
        schema_config = self.db_manager.get_schema_config(connection_id)
        if schema_config:
            self.db_manager.update_schema_config(schema_config['id'], config)
        else:
            # Get user_id from connection details
            connection = self.db_manager.get_connection(connection_id)
            if not connection:
                raise ValueError(f"Connection {connection_id} not found")
            
            self.db_manager.create_schema_config(
                connection_id=connection_id,
                user_id=connection['user_id'],
                config=config
            )

    def update_field_description(self, connection_id: str, table: str, field: str, description: str) -> None:
        """Update description for a specific field in the schema."""
        config = self.load_config(connection_id)
        if config and table in config['tables'] and field in config['tables'][table]['fields']:
            config['tables'][table]['fields'][field]['description'] = description
            self.save_config(connection_id, config)

    def update_table_description(self, connection_id: str, table: str, description: str) -> None:
        """Update description for a specific table in the schema."""
        config = self.load_config(connection_id)
        if config and table in config['tables']:
            config['tables'][table]['description'] = description
            self.save_config(connection_id, config)

    def update_business_context(self, connection_id: str, description: str, key_concepts: list) -> None:
        """Update business context in the schema configuration."""
        config = self.load_config(connection_id)
        if config:
            if 'business_context' not in config:
                config['business_context'] = {}
            config['business_context']['description'] = description
            config['business_context']['key_concepts'] = key_concepts
            self.save_config(connection_id, config)

    def get_tables(self, connection_id: str) -> list:
        """Get list of tables from schema configuration."""
        config = self.load_config(connection_id)
        return list(config['tables'].keys()) if config else []

    def get_fields(self, connection_id: str, table: str) -> list:
        """Get list of fields for a specific table."""
        config = self.load_config(connection_id)
        if config and table in config['tables']:
            return list(config['tables'][table]['fields'].keys())
        return []

    def get_field_info(self, connection_id: str, table: str, field: str) -> Dict[str, Any]:
        """Get detailed information about a specific field."""
        config = self.load_config(connection_id)
        if config and table in config['tables'] and field in config['tables'][table]['fields']:
            return config['tables'][table]['fields'][field]
        return {}

    def migrate_legacy_config(self, connection_id: str, legacy_type: str) -> bool:
        """Migrate a legacy database-type config to the new connection-based system."""
        try:
            legacy_path = os.path.join(self.config_dir, f"{legacy_type}_schema_config.yaml")
            if not os.path.exists(legacy_path):
                return False
            
            with open(legacy_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Get user_id from connection
            connection = self.db_manager.get_connection(connection_id)
            if not connection:
                return False
            
            # Create new schema config
            self.db_manager.create_schema_config(
                connection_id=connection_id,
                user_id=connection['user_id'],
                config=config
            )
            
            return True
        except Exception:
            return False
