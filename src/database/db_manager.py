"""Database manager for handling all database operations."""
import os
import json
import logging
import streamlit as st
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from .models import Base, User, Connection, SchemaConfig
import snowflake.connector
import traceback
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@st.cache_resource
def get_database_manager():
    """Get or create DatabaseManager instance."""
    return DatabaseManager()

class DatabaseManager:
    """Database manager class."""
    def __init__(self):
        """Initialize database manager."""
        logger.info("Starting DatabaseManager initialization")
        logger.info(f"Environment variables: {[k for k in os.environ.keys()]}")
        
        # Try different ways to get DATABASE_URL
        url = os.getenv('DATABASE_URL')
        logger.info(f"DATABASE_URL from os.getenv: {'set' if url else 'not set'}")
        
        if not url:
            # Try getting from streamlit secrets
            try:
                url = st.secrets["DATABASE_URL"]
                logger.info("Found DATABASE_URL in streamlit secrets")
            except Exception as e:
                logger.error(f"Error getting DATABASE_URL from streamlit secrets: {str(e)}")
        
        if not url:
            logger.error("DATABASE_URL not found in environment or secrets")
            raise ValueError("DATABASE_URL environment variable is not set")
            
        logger.info("DatabaseManager URL obtained, creating engine")
        self.engine = create_engine(url)
        logger.info("Engine created, setting up tables")
        Base.metadata.create_all(self.engine)
        logger.info("Tables set up, creating session maker")
        self.Session = sessionmaker(bind=self.engine)
        logger.info("DatabaseManager initialization complete")

    def _get_snowflake_connection(self, config: Dict) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection using connection config."""
        # Mask sensitive info in logs
        safe_config = {k: '***' if k in ['password', 'private_key_path'] else v 
                    for k, v in config.items()}
        logger.info("Attempting to connect to Snowflake with config: " + 
                json.dumps(safe_config, indent=2))
        
        try:
            # Base connection parameters
            connection_params = {
                'account': config['account'],
                'user': config['username'],
                'database': config['database'],
                'warehouse': config['warehouse'],
                'schema': config['schema']
            }
            
            # Determine authentication method
            auth_type = config.get('auth_type', 'password')  # Default to password for backward compatibility
            
            if auth_type == 'private_key':
                # Private key authentication
                private_key_path = config.get('private_key_path')
                if not private_key_path:
                    raise ValueError("Private key path not specified in config")
                
                # Try to get private key from environment or secrets
                try:
                    # First try environment variable
                    private_key_content = os.getenv(private_key_path)
                    
                    # If not found, try streamlit secrets
                    if not private_key_content:
                        try:
                            private_key_content = st.secrets[private_key_path]
                        except Exception:
                            pass
                    
                    # If still not found, treat as file path
                    if not private_key_content:
                        if os.path.exists(private_key_path):
                            with open(private_key_path, "rb") as key_file:
                                private_key_content = key_file.read()
                        else:
                            raise ValueError(f"Private key not found: {private_key_path}")
                    
                    # If it's a string (from env/secrets), process it carefully
                    if isinstance(private_key_content, str):
                        # Handle potential newline issues in environment variables
                        private_key_content = private_key_content.replace('\\n', '\n')
                        private_key_content = private_key_content.encode('utf-8')
                    
                    logger.info(f"Private key content length: {len(private_key_content)} bytes")
                    logger.info(f"Private key starts with: {str(private_key_content[:50])}")
                    
                    # Parse the private key using cryptography library
                    from cryptography.hazmat.primitives import serialization
                    from cryptography.hazmat.primitives.serialization import load_pem_private_key
                    
                    # Load the PEM private key
                    private_key_obj = load_pem_private_key(
                        private_key_content,
                        password=None  # We generated unencrypted key
                    )
                    
                    # Convert to DER format (binary) that Snowflake expects
                    private_key_der = private_key_obj.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    
                    connection_params['private_key'] = private_key_der
                    logger.info("Using private key authentication")
                    
                except Exception as e:
                    logger.error(f"Failed to load private key: {str(e)}")
                    logger.error(f"Private key content preview: {str(private_key_content[:100]) if 'private_key_content' in locals() else 'Not loaded'}")
                    raise ValueError(f"Failed to load private key: {str(e)}")
                    
            else:
                # Password authentication (default)
                connection_params['password'] = config['password']
                logger.info("Using password authentication")
            
            # Create connection
            conn = snowflake.connector.connect(**connection_params)
            logger.info("Successfully connected to Snowflake")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    def introspect_schema(self, connection_id: str) -> Optional[Dict]:
        """Query Snowflake metadata and build v2 schema configuration."""
        try:
            logger.info(f"Starting schema introspection for connection: {connection_id}")
            
            # Get connection config
            connection = self.get_connection(connection_id)
            if not connection:
                logger.error("Connection not found")
                return None
            
            logger.info("Retrieved connection config, attempting Snowflake connection")
            
            # Connect to Snowflake
            try:
                conn = self._get_snowflake_connection(connection['config'])
                cursor = conn.cursor()
                
                try:
                    logger.info("Querying for tables and views")
                    # Get table list (includes both tables and views)
                    cursor.execute("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = CURRENT_SCHEMA()
                        AND table_type IN ('BASE TABLE', 'VIEW')
                    """)
                    tables = cursor.fetchall()
                    logger.info(f"Found tables/views: {[t[0] for t in tables]}")
                    
                    # Create v2 schema structure (much cleaner!)
                    schema_config = {
                        "version": "2.0",
                        "tables": {},
                        "business_context": {
                            "description": "",
                            "key_concepts": []
                        },
                        "query_guidelines": {
                            "optimization_rules": [
                                "Always fully qualify column names with table aliases when multiple tables are involved in a query to avoid ambiguity.",
                                "Include IS_DELETED = FALSE filter in WHERE clauses to ensure only active records are analyzed.",
                                "When aggregating data, add appropriate GROUP BY clauses that include all non-aggregated columns in the SELECT statement."
                            ]
                        }
                    }
                    
                    # Get primary key information (IMPROVED!)
                    logger.info("Querying primary keys")
                    pk_dict = {}
                    try:
                        cursor.execute("""
                            SELECT 
                                tc.table_name,
                                kcu.column_name
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu 
                                ON tc.constraint_name = kcu.constraint_name
                                AND tc.table_schema = kcu.table_schema
                            WHERE tc.table_schema = CURRENT_SCHEMA()
                            AND tc.constraint_type = 'PRIMARY KEY'
                        """)
                        primary_keys = cursor.fetchall()
                        for table_name, column_name in primary_keys:
                            if table_name not in pk_dict:
                                pk_dict[table_name] = set()
                            pk_dict[table_name].add(column_name)
                        logger.info(f"Found primary keys: {pk_dict}")
                    except Exception as pk_error:
                        logger.warning(f"Could not query primary keys (insufficient permissions): {str(pk_error)}")
                        logger.info("Continuing without primary key information")
                        pk_dict = {}
                    
                    # Get column information for each table/view
                    for table_name, _ in tables:
                        logger.info(f"Querying columns for table: {table_name}")
                        cursor.execute(f"""
                            SELECT 
                                column_name,
                                data_type,
                                is_nullable,
                                column_default
                            FROM information_schema.columns
                            WHERE table_name = %s
                            AND table_schema = CURRENT_SCHEMA()
                            ORDER BY ordinal_position
                        """, (table_name,))
                        columns = cursor.fetchall()
                        logger.info(f"Found columns for {table_name}: {[c[0] for c in columns]}")
                        
                        # Add table to v2 schema (direct structure!)
                        schema_config["tables"][table_name] = {
                            "description": "",  # Will be filled by user
                            "fields": {}
                        }
                        
                        # Add columns to table
                        table_pks = pk_dict.get(table_name, set())
                        for col_name, data_type, nullable, default in columns:
                            schema_config["tables"][table_name]["fields"][col_name] = {
                                "type": data_type,
                                "nullable": nullable == "YES",
                                "primary_key": col_name in table_pks,  # FIXED!
                                "description": ""  # Will be filled by user
                            }
                    
                    logger.info(f"Schema introspection successful. Created v2.0 config with {len(schema_config['tables'])} tables")
                    return schema_config
                    
                finally:
                    logger.info("Closing cursor")
                    cursor.close()
                    
            finally:
                if 'conn' in locals():
                    logger.info("Closing Snowflake connection")
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error during schema introspection: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    def smart_schema_refresh(self, connection_id: str) -> bool:
        """Refresh schema structure while preserving manual annotations."""
        try:
            logger.info(f"Starting smart schema refresh for connection: {connection_id}")
            
            # 1. Get current config with annotations
            current_schema_config = self.get_schema_config(connection_id)
            
            # 2. If no current config exists, just do normal introspection
            if not current_schema_config:
                logger.info("No existing config found, doing full introspection")
                fresh_config = self.introspect_schema(connection_id)
                if fresh_config:
                    return self.update_schema_config(connection_id, fresh_config)
                return False
            
            # 3. Get fresh structure from database
            logger.info("Getting fresh schema structure from database")
            fresh_config = self.introspect_schema(connection_id)
            if not fresh_config:
                logger.error("Failed to introspect fresh schema")
                return False
            
            # 4. Merge: Keep annotations, update structure
            logger.info("Merging fresh structure with existing annotations")
            merged_config = self._merge_schema_configs(
                current_schema_config['config'], 
                fresh_config
            )
            
            # 5. Update with merged result
            success = self.update_schema_config(connection_id, merged_config)
            if success:
                logger.info("Smart schema refresh completed successfully")
            else:
                logger.error("Failed to update schema config with merged result")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during smart schema refresh: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def _merge_schema_configs(self, current: dict, fresh: dict) -> dict:
        """Merge fresh schema structure with existing annotations."""
        try:
            logger.info(f"Merging configs - Current version: {current.get('version', 'unknown')}, Fresh version: {fresh.get('version', 'unknown')}")
            
            # Start with fresh structure as base (gets new tables, updated types, etc.)
            merged = fresh.copy()
            
            # Preserve business context and query guidelines from current
            if current.get('business_context'):
                merged['business_context'] = current['business_context']
                logger.info("Preserved business context")
            
            if current.get('query_guidelines'):
                merged['query_guidelines'] = current['query_guidelines']
                logger.info("Preserved query guidelines")
            
            # For each table in fresh structure, preserve annotations
            current_tables = current.get('tables', {})
            fresh_tables = fresh.get('tables', {})
            
            for table_name, fresh_table in fresh_tables.items():
                current_table = current_tables.get(table_name, {})
                
                # Preserve table description
                if current_table.get('description'):
                    merged['tables'][table_name]['description'] = current_table['description']
                    logger.info(f"Preserved description for table: {table_name}")
                
                # For each field, preserve descriptions
                current_fields = current_table.get('fields', {})
                fresh_fields = fresh_table.get('fields', {})
                
                for field_name, fresh_field in fresh_fields.items():
                    current_field = current_fields.get(field_name, {})
                    
                    # Preserve field description annotation
                    if current_field.get('description'):
                        merged['tables'][table_name]['fields'][field_name]['description'] = current_field['description']
                        logger.info(f"Preserved description for field: {table_name}.{field_name}")
            
            # Log summary
            preserved_table_descs = sum(1 for t in merged.get('tables', {}).values() if t.get('description'))
            preserved_field_descs = sum(
                sum(1 for f in table.get('fields', {}).values() if f.get('description'))
                for table in merged.get('tables', {}).values()
            )
            
            logger.info(f"Merge complete - Preserved {preserved_table_descs} table descriptions and {preserved_field_descs} field descriptions")
            
            return merged
            
        except Exception as e:
            logger.error(f"Error during config merge: {str(e)}")
            # If merge fails, return fresh config as fallback
            logger.warning("Merge failed, returning fresh config as fallback")
            return fresh
    
    def add_user(self, username: str, password_hash: str) -> Tuple[Optional[str], Optional[str]]:
        """Add new user to database."""
        session = self.Session()
        try:
            logger.info(f"Adding new user: {username}")
            user = User(email=username, password_hash=password_hash)
            session.add(user)
            session.commit()
            logger.info(f"User added successfully with ID: {user.id}")
            return user.id, None
        except IntegrityError as e:
            # Check if it's a duplicate email error
            if "email" in str(e).lower() and "unique" in str(e).lower():
                logger.error(f"Email already exists: {username}")
                return None, "Email already exists"
            logger.error(f"Database integrity error: {str(e)}")
            return None, "Database error occurred"
        except Exception as e:
            logger.error(f"Error adding user: {str(e)}")
            return None, "An unexpected error occurred"
        finally:
            session.rollback()
            session.close()

    def get_user(self, username: str) -> Optional[Dict]:
        """Get user by username."""
        session = self.Session()
        try:
            logger.info(f"Getting user: {username}")
            user = session.query(User).filter(User.email == username).first()
            if user:
                logger.info(f"User found with ID: {user.id}")
                return {
                    'id': user.id,
                    'username': user.email,
                    'password_hash': user.password_hash,
                    'is_admin': user.is_admin  # Add is_admin to the returned user data
                }
            logger.info("User not found")
            return None
        finally:
            session.close()

    def add_connection(self, user_id: str, name: str, type_: str, config: str) -> Optional[str]:
        """Add new connection to database."""
        session = self.Session()
        try:
            logger.info(f"Adding connection for user {user_id}: {name}")
            connection = Connection(
                user_id=user_id,
                name=name,
                type=type_,
                config=json.loads(config),
                last_used=datetime.utcnow()
            )
            session.add(connection)
            session.commit()
            
            # Get connection ID
            connection_id = connection.id
            logger.info(f"Connection added with ID: {connection_id}")
            
            # Introspect schema for new connection
            logger.info("Starting schema introspection for new connection")
            schema_config = self.introspect_schema(connection_id)
            if schema_config:
                logger.info(f"Updating schema config with introspected schema: {json.dumps(schema_config, indent=2)}")
                if self.update_schema_config(connection_id, schema_config):
                    logger.info("Schema config updated successfully")
                else:
                    logger.error("Failed to update schema config")
            else:
                logger.warning("No schema config returned from introspection")
            
            return connection_id
        except Exception as e:
            logger.error(f"Error adding connection: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            session.rollback()
            return None
        finally:
            session.close()

    def get_user_connections(self, user_id: str) -> List[Dict]:
        """Get all connections for a user."""
        session = self.Session()
        try:
            logger.info(f"Getting connections for user: {user_id}")
            connections = session.query(Connection).filter(Connection.user_id == user_id).all()
            logger.info(f"Found {len(connections)} connections")
            return [{
                'id': conn.id,
                'name': conn.name,
                'type': conn.type,
                'config': conn.config
            } for conn in connections]
        finally:
            session.close()

    def get_connection(self, connection_id: str) -> Optional[Dict]:
        """Get connection by ID."""
        session = self.Session()
        try:
            logger.info(f"Getting connection: {connection_id}")
            connection = session.query(Connection).filter(Connection.id == connection_id).first()
            if connection:
                logger.info("Connection found")
                return {
                    'id': connection.id,
                    'name': connection.name,
                    'type': connection.type,
                    'config': connection.config
                }
            logger.info("Connection not found")
            return None
        finally:
            session.close()

    def update_schema_config(self, connection_id: str, config: Dict[str, Any]) -> bool:
        """Update schema configuration for a connection."""
        session = self.Session()
        try:
            logger.info(f"Updating schema config for connection: {connection_id}")
            
            connection = session.query(Connection).filter(Connection.id == connection_id).first()
            if not connection:
                logger.error("Connection not found")
                return False
                
            schema_config = session.query(SchemaConfig).filter(
                SchemaConfig.connection_id == connection_id
            ).first()
            
            if schema_config:
                logger.info("Updating existing schema config")
                # Use the new config values directly
                schema_config.config = config
                schema_config.last_modified = datetime.utcnow()
            else:
                logger.info("Creating new schema config")
                schema_config = SchemaConfig(
                    connection_id=connection_id,
                    user_id=connection.user_id,
                    config=config
                )
                session.add(schema_config)
            
            session.commit()
            logger.info("Schema config updated successfully")
            logger.info(f"Final config in database: {json.dumps(schema_config.config, indent=2)}")
            return True
        except Exception as e:
            logger.error(f"Error updating schema config: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            session.rollback()
            return False
        finally:
            session.close()

    def get_schema_config(self, connection_id: str) -> Optional[Dict]:
        """Get schema configuration for a connection."""
        session = self.Session()
        try:
            logger.info(f"Getting schema config for connection: {connection_id}")
            schema_config = session.query(SchemaConfig).filter(
                SchemaConfig.connection_id == connection_id
            ).first()
            
            if schema_config:
                logger.info("Schema config found")
                logger.info(f"Config content: {json.dumps(schema_config.config, indent=2)}")
                return {
                    'id': schema_config.id,
                    'config': schema_config.config
                }
            logger.info("Schema config not found")
            return None
        finally:
            session.close()
