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
        url = os.getenv('DATABASE_URL')
        if not url:
            raise ValueError("DATABASE_URL environment variable is not set")
            
        logger.info(f"Initializing DatabaseManager")
        self.engine = create_engine(url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def _get_snowflake_connection(self, config: Dict) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection using connection config."""
        return snowflake.connector.connect(
            account=config['account'],
            user=config['username'],
            password=config['password'],
            database=config['database'],
            warehouse=config['warehouse'],
            schema=config['schema']
        )

    def introspect_schema(self, connection_id: str) -> Optional[Dict]:
        """Query Snowflake metadata and build schema configuration."""
        try:
            logger.info(f"Introspecting schema for connection: {connection_id}")
            
            # Get connection config
            connection = self.get_connection(connection_id)
            if not connection:
                logger.error("Connection not found")
                return None
            
            # Connect to Snowflake
            conn = self._get_snowflake_connection(connection['config'])
            cursor = conn.cursor()
            
            try:
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
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error during schema introspection: {str(e)}")
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
            
            # Introspect schema for new connection
            schema_config = self.introspect_schema(connection_id)
            if schema_config:
                logger.info(f"Updating schema config with introspected schema: {json.dumps(schema_config, indent=2)}")
                self.update_schema_config(connection_id, schema_config)
            else:
                logger.warning("No schema config returned from introspection")
            
            logger.info(f"Connection added with ID: {connection_id}")
            return connection_id
        except Exception as e:
            logger.error(f"Error adding connection: {str(e)}")
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
            logger.info(f"New config: {json.dumps(config, indent=2)}")
            
            connection = session.query(Connection).filter(Connection.id == connection_id).first()
            if not connection:
                logger.error("Connection not found")
                return False
                
            schema_config = session.query(SchemaConfig).filter(
                SchemaConfig.connection_id == connection_id
            ).first()
            
            if schema_config:
                logger.info("Updating existing schema config")
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
