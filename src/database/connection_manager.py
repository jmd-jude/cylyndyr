"""Connection management with database-specific schema inspection."""
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
import streamlit as st
from datetime import datetime
import os

from src.database.db_manager import DatabaseManager
from src.database.schema_inspector import inspect_database

class ConnectionManager:
    def __init__(self):
        """Initialize connection manager with database manager."""
        self.db_manager = DatabaseManager()
        self._active_engines = {}

    def create_snowflake_connection(self, user_id: str, name: str, credentials: Dict[str, str]) -> Optional[str]:
        """
        Create a new Snowflake connection configuration.
        Returns connection ID if successful, None otherwise.
        """
        try:
            # Validate required credentials
            required_fields = ['account', 'user', 'password', 'database', 'warehouse', 'schema']
            if not all(field in credentials and credentials[field] for field in required_fields):
                missing = [f for f in required_fields if not credentials.get(f)]
                raise ValueError(f"Missing or empty required fields: {missing}")

            # Create connection in database
            connection_id = self.db_manager.create_connection(
                user_id=user_id,
                name=name,
                conn_type='snowflake',
                config=credentials
            )

            if connection_id:
                # Test connection and inspect schema
                engine = self.get_connection_engine(connection_id)
                if engine:
                    try:
                        # Inspect database schema
                        config = inspect_database(
                            db_type="snowflake",
                            **credentials
                        )
                        
                        # Add database and schema info to config
                        if 'database_config' not in config:
                            config['database_config'] = {}
                        config['database_config'].update({
                            'database': credentials['database'],
                            'schema': credentials['schema']
                        })
                        
                        # Update table names to be fully qualified
                        if 'tables' in config:
                            qualified_tables = {}
                            for table_name, table_info in config['tables'].items():
                                qualified_name = f"{credentials['database']}.{credentials['schema']}.{table_name}"
                                qualified_tables[qualified_name] = table_info
                            config['tables'] = qualified_tables
                        
                        # Store schema config
                        self.db_manager.create_schema_config(
                            connection_id=connection_id,
                            user_id=user_id,
                            config=config
                        )
                        st.success("Schema configuration created!")
                    except Exception as e:
                        st.error(f"Error creating schema config: {str(e)}")
                        # Clean up failed connection
                        self.db_manager.delete_connection(connection_id)
                        return None
                else:
                    # Clean up failed connection
                    self.db_manager.delete_connection(connection_id)
                    return None

            return connection_id
        except Exception as e:
            st.error(f"Error creating connection: {str(e)}")
            return None

    def get_connection_engine(self, connection_id: str) -> Optional[Any]:
        """
        Get SQLAlchemy engine for a connection.
        Creates new engine if none exists, otherwise returns cached engine.
        """
        try:
            # Return cached engine if it exists
            if connection_id in self._active_engines:
                return self._active_engines[connection_id]

            # Get connection details
            connection = self.db_manager.get_connection(connection_id)
            if not connection:
                raise ValueError(f"Connection {connection_id} not found")

            # Create appropriate engine based on connection type
            if connection['type'] == 'snowflake':
                config = connection['config']
                # Validate all required fields are non-empty
                required_fields = ['account', 'user', 'password', 'database', 'warehouse', 'schema']
                if not all(field in config and config[field] for field in required_fields):
                    missing = [f for f in required_fields if not config.get(f)]
                    raise ValueError(f"Connection config missing or empty required fields: {missing}")

                engine = create_engine(URL(
                    account=config['account'],
                    user=config['user'],
                    password=config['password'],
                    database=config['database'],
                    warehouse=config['warehouse'],
                    schema=config['schema']
                ))
            else:
                raise ValueError(f"Unsupported connection type: {connection['type']}")

            # Test the engine
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Cache and return the engine
            self._active_engines[connection_id] = engine
            
            # Update last used timestamp
            self.db_manager.update_connection_last_used(connection_id)
            
            return engine
        except Exception as e:
            st.error(f"Error getting connection engine: {str(e)}")
            return None

    def test_connection(self, connection_id: str) -> bool:
        """Test if a connection is valid by attempting to connect."""
        try:
            engine = self.get_connection_engine(connection_id)
            if engine is None:
                return False
                
            # Try to connect and execute a simple query
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            st.error(f"Connection test failed: {str(e)}")
            return False

    def migrate_existing_connection(self) -> Optional[str]:
        """
        Migrate existing Streamlit secrets/env vars Snowflake connection 
        to the new connection management system.
        """
        try:
            # Try environment variables first
            credentials = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
                'user': os.getenv('SNOWFLAKE_USER', ''),
                'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
                'database': os.getenv('SNOWFLAKE_DATABASE', ''),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', ''),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', '')
            }

            # Check if we have valid environment variables
            if all(credentials.values()):
                st.write("Using environment variables for credentials")
            else:
                # Fall back to Streamlit secrets
                try:
                    credentials = {
                        'account': st.secrets.snowflake.account,
                        'user': st.secrets.snowflake.user,
                        'password': st.secrets.snowflake.password,
                        'database': st.secrets.snowflake.database,
                        'warehouse': st.secrets.snowflake.warehouse,
                        'schema': st.secrets.snowflake.schema
                    }
                    # Check if secrets contain placeholder values
                    if any(v.startswith('your-') for v in credentials.values()):
                        raise ValueError("Secrets contain placeholder values")
                    st.write("Using Streamlit secrets for credentials")
                except Exception as e:
                    st.error(f"No valid credentials found in environment or secrets: {str(e)}")
                    return None

            # Get or create default user
            default_user = self.db_manager.get_user_by_email("default@cylyndyr.com")
            if default_user:
                user_id = default_user['id']
                st.write("Using existing default user")
            else:
                user_id = self.db_manager.create_user(
                    email="default@cylyndyr.com",
                    name="Default User"
                )
                st.write("Created new default user")

            # Create the connection
            connection_id = self.create_snowflake_connection(
                user_id=user_id,
                name="Default Snowflake Connection",
                credentials=credentials
            )

            return connection_id
        except Exception as e:
            st.error(f"Error migrating existing connection: {str(e)}")
            return None

    def close_all_connections(self):
        """Close all active database connections."""
        for engine in self._active_engines.values():
            engine.dispose()
        self._active_engines.clear()
