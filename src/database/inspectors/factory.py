"""Factory for creating database schema inspectors."""
from typing import Dict, Any, Type, List
from .base import BaseSchemaInspector
from .snowflake import SnowflakeInspector

class InspectorFactory:
    """Factory class for creating database schema inspectors."""
    
    # Registry of supported database types and their inspector classes
    _inspectors: Dict[str, Type[BaseSchemaInspector]] = {
        'snowflake': SnowflakeInspector
    }
    
    # Required parameters for each database type
    _required_params: Dict[str, List[str]] = {
        'snowflake': ['username', 'password', 'account', 'warehouse', 'database', 'schema']
    }
    
    @classmethod
    def register_inspector(cls, db_type: str, inspector_class: Type[BaseSchemaInspector], 
                         required_params: List[str] = None):
        """Register a new inspector class for a database type.
        
        Args:
            db_type: Database type identifier (e.g., 'postgres', 'mysql')
            inspector_class: Inspector class for the database type
            required_params: List of required connection parameters
        """
        db_type = db_type.lower()
        cls._inspectors[db_type] = inspector_class
        if required_params:
            cls._required_params[db_type] = required_params
    
    @classmethod
    def create_inspector(cls, connection_config: Dict[str, Any]) -> BaseSchemaInspector:
        """Create an appropriate inspector instance for the database type.
        
        Args:
            connection_config: Dictionary containing connection configuration
                Must include a 'type' key identifying the database type
                
        Returns:
            An instance of the appropriate schema inspector
            
        Raises:
            ValueError: If database type is not supported or config is invalid
        """
        # Validate database type
        db_type = connection_config.get('type', '').lower()
        if not db_type:
            raise ValueError("Database type not specified in connection configuration")
        
        if db_type not in cls._inspectors:
            supported = ', '.join(cls._inspectors.keys())
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Supported types are: {supported}"
            )
        
        # Validate required parameters
        required = cls._required_params.get(db_type, [])
        missing = [param for param in required if not connection_config.get(param)]
        if missing:
            raise ValueError(
                f"Missing required parameters for {db_type}: {', '.join(missing)}"
            )
        
        # Create inspector instance
        inspector_class = cls._inspectors[db_type]
        return inspector_class(connection_config)
    
    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Get list of supported database types.
        
        Returns:
            List of supported database type identifiers
        """
        return list(cls._inspectors.keys())
    
    @classmethod
    def get_required_params(cls, db_type: str) -> List[str]:
        """Get required parameters for a database type.
        
        Args:
            db_type: Database type identifier
            
        Returns:
            List of required parameter names
            
        Raises:
            ValueError: If database type is not supported
        """
        db_type = db_type.lower()
        if db_type not in cls._inspectors:
            supported = ', '.join(cls._inspectors.keys())
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Supported types are: {supported}"
            )
        
        return cls._required_params.get(db_type, [])
    
    @classmethod
    def validate_config(cls, connection_config: Dict[str, Any]) -> List[str]:
        """Validate connection configuration.
        
        Args:
            connection_config: Connection configuration to validate
            
        Returns:
            List of validation errors, empty if valid
        """
        errors = []
        
        # Check database type
        db_type = connection_config.get('type', '').lower()
        if not db_type:
            errors.append("Database type not specified")
            return errors
        
        if db_type not in cls._inspectors:
            supported = ', '.join(cls._inspectors.keys())
            errors.append(
                f"Unsupported database type: {db_type}. "
                f"Supported types are: {supported}"
            )
            return errors
        
        # Check required parameters
        required = cls._required_params.get(db_type, [])
        for param in required:
            if not connection_config.get(param):
                errors.append(f"Missing required parameter: {param}")
        
        return errors

# Example usage:
# inspector = InspectorFactory.create_inspector({
#     'type': 'snowflake',
#     'username': '...',
#     'password': '...',
#     'account': '...',
#     'warehouse': '...',
#     'database': '...',
#     'schema': '...'
# })
# schema = inspector.inspect_schema()
