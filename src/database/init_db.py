import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.database.models import init_db, User, Connection, SchemaConfig
from src.database.db_manager import DatabaseManager
import yaml

def initialize_database():
    """Initialize the SQLite database and create default user if needed."""
    print("Initializing database...")
    db_manager = DatabaseManager()
    
    # Create default user if doesn't exist
    default_user = db_manager.get_user_by_email("default@cylyndyr.com")
    if not default_user:
        print("Creating default user...")
        user_id = db_manager.create_user(
            email="default@cylyndyr.com",
            name="Default User"
        )
        print(f"Created default user with ID: {user_id}")
    else:
        user_id = default_user['id']
        print(f"Using existing default user with ID: {user_id}")
    
    # Check for existing Snowflake connection
    connections = db_manager.get_user_connections(user_id)
    if not connections:
        print("No existing connections found.")
        print("To create a connection, run the app and it will automatically migrate")
        print("your existing Snowflake connection from environment variables or secrets.")
    else:
        print(f"Found {len(connections)} existing connection(s):")
        for conn in connections:
            print(f"- {conn['name']} (ID: {conn['id']})")
    
    # Check for existing schema configs
    config_dir = os.path.join(project_root, "schema_configs")
    if os.path.exists(config_dir):
        yaml_files = [f for f in os.listdir(config_dir) if f.endswith('.yaml')]
        if yaml_files:
            print("\nFound existing schema configuration files:")
            for yaml_file in yaml_files:
                print(f"- {yaml_file}")
            print("\nThese will be automatically migrated when you run the app")
            print("and create the corresponding connections.")
    
    print("\nDatabase initialization complete!")
    print("\nNext steps:")
    print("1. Run the app normally")
    print("2. Your existing connection will be automatically migrated")
    print("3. You can then add additional connections through the UI")

if __name__ == "__main__":
    initialize_database()
