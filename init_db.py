"""Initialize database with required tables."""
import os
from dotenv import load_dotenv
from src.database.models import init_db

def main():
    """Initialize the database."""
    # Load environment variables
    load_dotenv()
    
    # Get database URL with SQLite fallback
    db_url = os.getenv('DATABASE_URL', 'sqlite:///cylyndyr.db')
    
    print("Initializing database...")
    print(f"Using database: {db_url}")
    
    init_db(db_url)
    
    print("Database initialization complete!")
    print("\nNext steps:")
    print("1. Run the app")
    print("2. Create your first connection")
    print("3. Start querying your data")

if __name__ == "__main__":
    main()
