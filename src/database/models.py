"""Database models."""
import os
from sqlalchemy import create_engine, Column, String, DateTime, ForeignKey, JSON, Table, MetaData, UniqueConstraint, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

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
    is_admin = Column(Boolean, default=False, nullable=False)  # New column for role-based access
    
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

class QueryHistory(Base):
    __tablename__ = 'query_history'
    
    id = Column(String, primary_key=True, default=generate_uuid)
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    connection_id = Column(String, ForeignKey('connections.id'), nullable=False)
    question = Column(String, nullable=False)
    generated_sql = Column(String, nullable=False)
    result_preview = Column(JSON)  # First 10 rows as JSON
    result_metadata = Column(JSON)  # {row_count, column_count, execution_time_ms}
    created_at = Column(DateTime, default=datetime.utcnow)
    is_favorite = Column(Boolean, default=False, nullable=False)
    query_hash = Column(String)  # For deduplication
    
    # Relationships
    user = relationship("User")
    connection = relationship("Connection")

class InteractionLog(Base):
    __tablename__ = 'interaction_logs'
    
    id = Column(String, primary_key=True, default=generate_uuid)
    user_id = Column(String, ForeignKey('users.id'), nullable=True)  # Nullable in case user not logged in
    connection_id = Column(String, ForeignKey('connections.id'), nullable=True)
    thread_id = Column(String, nullable=False)
    interaction_type = Column(String, nullable=False)  # 'query_generation', 'query_execution', 'analysis', etc.
    database_name = Column(String, nullable=True)  # User's connection name
    payload = Column(JSON, nullable=False)  # All the detailed data
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User")
    connection = relationship("Connection")

def init_db(db_url):
    """Initialize the database and create all tables."""
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine
