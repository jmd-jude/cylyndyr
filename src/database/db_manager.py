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
from psycopg2.errors import UniqueViolation
from .models import Base, User, Connection, SchemaConfig
import snowflake.connector
import traceback

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
        logger.info("Attempting to connect to Snowflake with config: " + 
                   json.dumps({k: '***' if k == 'password' else v 
                             for k, v in config.items()}, indent=2))
        try:
            conn = snowflake.connector.connect(
                account=config['account'],
                user=config['username'],
                password=config['password'],
                database=config['database'],
                warehouse=config['warehouse'],
                schema=config['schema']
            )
            logger.info("Successfully connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    def introspect_schema(self, connection_id: str) -> Optional[Dict]:
        """Query Snowflake metadata and build schema configuration."""
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
                    logger.info("Querying for tables")
                    # Get table list
                    cursor.execute("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = CURRENT_SCHEMA()
                        AND table_type = 'BASE TABLE'
                    """)
                    tables = cursor.fetchall()
                    logger.info(f"Found tables: {[t[0] for t in tables]}")
                    
                    schema_config = {
                        "base_schema": {
                            "tables": {}
                        },
                        "business_context": {
                            "description": "",
                            "key_concepts": [],
                            "table_descriptions": {}
                        },
                        "query_guidelines": {
                            "optimization_rules": []
                        }
                    }
                    
                    # Get column information for each table
                    for table_name, _ in tables:
                        logger.info(f"Querying columns for table: {table_name}")
                        cursor.execute(f"""
                            SELECT 
                                column_name,
                                data_type,
                                is_nullable,
                                column_default,
                                is_identity
                            FROM information_schema.columns
                            WHERE table_name = %s
                            AND table_schema = CURRENT_SCHEMA()
                            ORDER BY ordinal_position
                        """, (table_name,))
                        columns = cursor.fetchall()
                        logger.info(f"Found columns for {table_name}: {[c[0] for c in columns]}")
                        
                        # Add table to schema
                        schema_config["base_schema"]["tables"][table_name] = {
                            "fields": {}
                        }
                        
                        # Add columns to table
                        for col_name, data_type, nullable, default, is_identity in columns:
                            schema_config["base_schema"]["tables"][table_name]["fields"][col_name] = {
                                "type": data_type,
                                "nullable": nullable == "YES",
                                "primary_key": is_identity == "YES"
                            }
                    
                    logger.info(f"Schema introspection successful. Config: {json.dumps(schema_config, indent=2)}")
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
            if isinstance(e.orig, UniqueViolation):
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
                    'password_hash': user.password_hash
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
                # Preserve existing business context and query guidelines
                existing_config = schema_config.config
                new_config = {
                    "base_schema": config["base_schema"],
                    "business_context": existing_config.get("business_context", {
                        "description": "",
                        "key_concepts": [],
                        "table_descriptions": {}
                    }),
                    "query_guidelines": existing_config.get("query_guidelines", {
                        "optimization_rules": []
                    })
                }
                schema_config.config = new_config
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
