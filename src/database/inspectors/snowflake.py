"""Snowflake-specific schema inspector."""
import snowflake.connector
from typing import Dict, Any, List
from .base import BaseSchemaInspector

class SnowflakeInspector(BaseSchemaInspector):
    """Snowflake-specific schema inspector implementation."""
    
    def _inspect_database(self) -> Dict[str, Any]:
        """Inspect Snowflake database schema.
        
        Returns:
            Dictionary containing tables, columns, and relationships
        """
        try:
            conn = snowflake.connector.connect(
                user=self.config['username'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema']
            )
            
            cursor = conn.cursor()
            
            # Get tables
            cursor.execute(f"SHOW TABLES IN {self.config['database']}.{self.config['schema']}")
            tables = cursor.fetchall()
            
            tables_dict = {}
            relationships = []
            
            # Process each table
            for table in tables:
                table_name = table[1]
                
                # Get columns
                cursor.execute(f"DESCRIBE TABLE {table_name}")
                columns = cursor.fetchall()
                
                # Add table to schema config
                tables_dict[table_name] = {
                    "fields": {
                        col[0]: {
                            "type": col[1],
                            "nullable": col[3] == "Y",
                            "default": col[4],
                            "primary_key": False,
                            "foreign_key": False
                        }
                        for col in columns
                    },
                    "row_count": None
                }
                
                # Get row count
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()
                    if row_count:
                        tables_dict[table_name]["row_count"] = row_count[0]
                except Exception:
                    pass
                
                # Try to get primary/foreign keys
                try:
                    cursor.execute(f"""
                        SELECT 
                            kcu.COLUMN_NAME,
                            CASE 
                                WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PK'
                                WHEN tc.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'FK'
                            END as KEY_TYPE,
                            ccu.TABLE_NAME as REFERENCED_TABLE,
                            ccu.COLUMN_NAME as REFERENCED_COLUMN
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                        LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                            ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                        LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                            ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                        WHERE tc.TABLE_NAME = '{table_name}'
                        AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY')
                    """)
                    keys = cursor.fetchall()
                    
                    for key in keys:
                        col_name, key_type, ref_table, ref_col = key
                        
                        # Update field info
                        field_info = tables_dict[table_name]["fields"][col_name]
                        if key_type == 'PK':
                            field_info["primary_key"] = True
                        elif key_type == 'FK':
                            field_info["foreign_key"] = True
                            # Add relationship
                            relationships.append({
                                "table": table_name,
                                "field": col_name,
                                "referenced_table": ref_table,
                                "referenced_field": ref_col
                            })
                except Exception:
                    pass
            
            cursor.close()
            conn.close()
            
            # Return in base_schema structure
            return {
                "tables": tables_dict,
                "relationships": relationships
            }
            
        except Exception as e:
            raise Exception(f"Error inspecting Snowflake schema: {str(e)}")
    
    def _create_query_guidelines(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create Snowflake-specific query guidelines.
        
        Args:
            schema: The base schema dictionary
            
        Returns:
            Dictionary containing Snowflake-specific query optimization guidelines
        """
        guidelines = super()._create_query_guidelines(schema)
        
        # Add Snowflake-specific optimization rules
        guidelines["optimization_rules"].extend([
            "Use CLUSTER BY for frequently filtered columns",
            "Consider materialized views for complex aggregations",
            "Leverage micro-partitions for large tables",
            "Use appropriate warehouse sizes for query complexity"
        ])
        
        # Add Snowflake-specific sections
        guidelines.update({
            "warehouse_optimization": {
                "sizing_rules": [
                    "Use larger warehouses for complex transformations",
                    "Scale down for simple queries",
                    "Consider multi-cluster for concurrent users"
                ],
                "caching_hints": [
                    "Leverage result caching for repeated queries",
                    "Use persisted query results when appropriate"
                ]
            },
            "materialization_hints": {
                "view_candidates": [],
                "clustering_keys": []
            }
        })
        
        return guidelines
    
    def _get_metadata(self) -> Dict[str, Any]:
        """Get Snowflake-specific metadata.
        
        Returns:
            Dictionary containing Snowflake-specific metadata
        """
        metadata = super()._get_metadata()
        metadata.update({
            "warehouse": self.config.get("warehouse", ""),
            "account": self.config.get("account", "")
        })
        return metadata
