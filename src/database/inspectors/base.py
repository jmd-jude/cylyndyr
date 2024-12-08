"""Base schema inspector for database connections."""
from typing import Dict, Any, Optional

class BaseSchemaInspector:
    """Base class for all database schema inspectors.
    
    This class defines the interface and common functionality for inspecting
    database schemas across different database types. Specific database
    implementations should inherit from this class.
    """
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize base schema inspector.
        
        Args:
            connection_config: Dictionary containing connection configuration
        """
        self.config = connection_config
        
    def inspect_schema(self) -> Dict[str, Any]:
        """Inspect database schema and return complete configuration.
        
        Returns:
            Dictionary containing:
            - metadata: Database metadata
            - base_schema: Raw schema information
            - business_context: Business-specific metadata
            - query_guidelines: Database-specific query optimization rules
        """
        base_schema = self._inspect_database()
        return {
            "metadata": self._get_metadata(),
            "base_schema": base_schema,
            "business_context": self._create_business_context(base_schema),
            "query_guidelines": self._create_query_guidelines(base_schema)
        }
    
    def _inspect_database(self) -> Dict[str, Any]:
        """Inspect database and return schema information.
        
        This method must be implemented by specific database inspectors.
        
        Returns:
            Dictionary containing tables, columns, and relationships
        
        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Database inspection not implemented")
    
    def _get_metadata(self) -> Dict[str, Any]:
        """Get database metadata.
        
        Returns:
            Dictionary containing database type, name, and version
        """
        return {
            "database": self.config.get("database", ""),
            "schema": self.config.get("schema", ""),
            "type": self.config.get("type", "unknown"),
            "version": "1.0"
        }
    
    def _create_business_context(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create initial business context structure.
        
        Args:
            schema: The base schema dictionary
            
        Returns:
            Dictionary containing business context template
        """
        return {
            "description": "",
            "key_concepts": [],
            "table_descriptions": {
                table_name: {
                    "description": "",
                    "fields": {
                        field_name: {
                            "description": "",
                            "business_rules": []
                        }
                        for field_name in table_info.get("fields", {}).keys()
                    }
                }
                for table_name, table_info in schema.get("tables", {}).items()
            }
        }
    
    def _create_query_guidelines(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create initial query guidelines structure.
        
        Args:
            schema: The base schema dictionary
            
        Returns:
            Dictionary containing query optimization guidelines
        """
        return {
            "optimization_rules": [
                "Prefer specific column selection over SELECT *",
                "Include WHERE clauses when possible",
                "Consider table sizes when ordering joins"
            ],
            "join_patterns": [],
            "performance_hints": []
        }
    
    def merge_with_existing(self, new_config: Dict[str, Any], existing_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge new schema config with existing config.
        
        Args:
            new_config: Newly inspected schema configuration
            existing_config: Existing schema configuration
            
        Returns:
            Merged configuration preserving user customizations
        """
        if not existing_config:
            return new_config
            
        # Preserve business context
        if "business_context" in existing_config:
            new_config["business_context"] = existing_config["business_context"]
            
            # Add new tables/fields to business context if they don't exist
            for table_name in new_config["base_schema"].get("tables", {}).keys():
                if table_name not in new_config["business_context"]["table_descriptions"]:
                    new_config["business_context"]["table_descriptions"][table_name] = {
                        "description": "",
                        "fields": {}
                    }
                
                for field_name in new_config["base_schema"]["tables"][table_name].get("fields", {}).keys():
                    if field_name not in new_config["business_context"]["table_descriptions"][table_name]["fields"]:
                        new_config["business_context"]["table_descriptions"][table_name]["fields"][field_name] = {
                            "description": "",
                            "business_rules": []
                        }
        
        # Preserve query guidelines while adding new optimization rules
        if "query_guidelines" in existing_config:
            existing_rules = set(existing_config["query_guidelines"].get("optimization_rules", []))
            new_rules = set(new_config["query_guidelines"].get("optimization_rules", []))
            new_config["query_guidelines"]["optimization_rules"] = list(existing_rules | new_rules)
            
            new_config["query_guidelines"]["join_patterns"] = existing_config["query_guidelines"].get("join_patterns", [])
            new_config["query_guidelines"]["performance_hints"] = existing_config["query_guidelines"].get("performance_hints", [])
        
        return new_config
