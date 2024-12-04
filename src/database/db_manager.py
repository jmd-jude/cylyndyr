from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import json
import yaml
from typing import Optional, List, Dict, Any

from src.database.models import init_db, User, Connection, SchemaConfig

class DatabaseManager:
    def __init__(self, db_url='sqlite:///cylyndyr.db'):
        """Initialize database manager with connection to SQLite database."""
        self.engine = init_db(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def create_user(self, email: str, name: str) -> Optional[str]:
        """Create a new user and return their ID."""
        session = self.Session()
        try:
            user = User(email=email, name=name)
            session.add(user)
            session.commit()
            return user.id
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user details by ID."""
        session = self.Session()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            if user:
                return {
                    'id': user.id,
                    'email': user.email,
                    'name': user.name,
                    'created_at': user.created_at,
                    'last_login': user.last_login
                }
            return None
        finally:
            session.close()

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user details by email."""
        session = self.Session()
        try:
            user = session.query(User).filter(User.email == email).first()
            if user:
                return {
                    'id': user.id,
                    'email': user.email,
                    'name': user.name,
                    'created_at': user.created_at,
                    'last_login': user.last_login
                }
            return None
        finally:
            session.close()

    def create_connection(self, user_id: str, name: str, conn_type: str, config: Dict) -> Optional[str]:
        """Create a new connection for a user."""
        session = self.Session()
        try:
            connection = Connection(
                user_id=user_id,
                name=name,
                type=conn_type,
                config=config
            )
            session.add(connection)
            session.commit()
            return connection.id
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def delete_connection(self, connection_id: str) -> bool:
        """Delete a connection and its associated schema config."""
        session = self.Session()
        try:
            # Delete associated schema config first
            schema_config = session.query(SchemaConfig).filter(
                SchemaConfig.connection_id == connection_id
            ).first()
            if schema_config:
                session.delete(schema_config)

            # Delete the connection
            connection = session.query(Connection).filter(
                Connection.id == connection_id
            ).first()
            if connection:
                session.delete(connection)
                session.commit()
                return True
            return False
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_user_connections(self, user_id: str) -> List[Dict]:
        """Get all connections for a user."""
        session = self.Session()
        try:
            connections = session.query(Connection).filter(Connection.user_id == user_id).all()
            return [{
                'id': conn.id,
                'name': conn.name,
                'type': conn.type,
                'config': conn.config,
                'created_at': conn.created_at,
                'last_used': conn.last_used
            } for conn in connections]
        finally:
            session.close()

    def get_connection(self, connection_id: str) -> Optional[Dict]:
        """Get connection details by ID."""
        session = self.Session()
        try:
            conn = session.query(Connection).filter(Connection.id == connection_id).first()
            if conn:
                return {
                    'id': conn.id,
                    'user_id': conn.user_id,
                    'name': conn.name,
                    'type': conn.type,
                    'config': conn.config,
                    'created_at': conn.created_at,
                    'last_used': conn.last_used
                }
            return None
        finally:
            session.close()

    def update_connection_last_used(self, connection_id: str):
        """Update the last_used timestamp of a connection."""
        session = self.Session()
        try:
            conn = session.query(Connection).filter(Connection.id == connection_id).first()
            if conn:
                conn.last_used = datetime.utcnow()
                session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def create_schema_config(self, connection_id: str, user_id: str, config: Dict) -> Optional[str]:
        """Create a new schema configuration."""
        session = self.Session()
        try:
            # Delete any existing schema config for this connection
            existing_config = session.query(SchemaConfig).filter(
                SchemaConfig.connection_id == connection_id
            ).first()
            if existing_config:
                session.delete(existing_config)

            schema_config = SchemaConfig(
                connection_id=connection_id,
                user_id=user_id,
                config=config
            )
            session.add(schema_config)
            session.commit()
            return schema_config.id
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_schema_config(self, connection_id: str) -> Optional[Dict]:
        """Get schema configuration for a connection."""
        session = self.Session()
        try:
            config = session.query(SchemaConfig).filter(
                SchemaConfig.connection_id == connection_id
            ).first()
            if config:
                return {
                    'id': config.id,
                    'connection_id': config.connection_id,
                    'user_id': config.user_id,
                    'config': config.config,
                    'last_modified': config.last_modified,
                    'created_at': config.created_at
                }
            return None
        finally:
            session.close()

    def update_schema_config(self, config_id: str, new_config: Dict) -> bool:
        """Update an existing schema configuration."""
        session = self.Session()
        try:
            config = session.query(SchemaConfig).filter(SchemaConfig.id == config_id).first()
            if config:
                config.config = new_config
                session.commit()
                return True
            return False
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def migrate_existing_config(self, user_id: str, connection_id: str, config_path: str):
        """Migrate existing YAML config to the database."""
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            
            self.create_schema_config(
                connection_id=connection_id,
                user_id=user_id,
                config=config_data
            )
            return True
        except Exception as e:
            print(f"Error migrating config: {str(e)}")
            return False
