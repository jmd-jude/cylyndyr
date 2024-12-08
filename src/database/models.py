"""Database models."""
import os
from sqlalchemy import create_engine, Column, String, DateTime, ForeignKey, JSON, Table, MetaData, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.sqlite import JSON
from datetime import datetime
import uuid
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

Base = declarative_base()

def generate_uuid():
    return str(uuid.uuid4())

class User(Base):
    __tablename__ = 'users'
    
    id = Column(String, primary_key=True, default=generate_uuid)
    email = Column(String, unique=True, nullable=False)
    name = Column(String)
    password_hash = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime)
    
    # Relationships
    connections = relationship("Connection", back_populates="user")
    schema_configs = relationship("SchemaConfig", back_populates="user")

class Connection(Base):
    __tablename__ = 'connections'
    
    id = Column(String, primary_key=True, default=generate_uuid)
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    config = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_used = Column(DateTime)
    
    # Relationships
    user = relationship("User", back_populates="connections")
    schema_configs = relationship("SchemaConfig", back_populates="connection")
    
    # Add unique constraint for name per user
    __table_args__ = (
        UniqueConstraint('user_id', 'name', name='uix_user_connection_name'),
    )

class SchemaConfig(Base):
    __tablename__ = 'schema_configs'
    
    id = Column(String, primary_key=True, default=generate_uuid)
    connection_id = Column(String, ForeignKey('connections.id'), nullable=False)
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    config = Column(JSON, nullable=False)
    last_modified = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="schema_configs")
    connection = relationship("Connection", back_populates="schema_configs")

def init_db(db_url=None):
    """Initialize the database and create all tables.
    
    Args:
        db_url: Optional database URL. If not provided, uses DATABASE_URL from environment
               or falls back to SQLite.
    """
    if not db_url:
        db_url = os.getenv('DATABASE_URL', 'sqlite:///cylyndyr.db')
    
    # Create engine with appropriate JSON type for PostgreSQL
    if db_url.startswith('postgresql'):
        from sqlalchemy.dialects.postgresql import JSON
    
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine
