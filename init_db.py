"""Initialize database with required tables."""
import os
from dotenv import load_dotenv
from src.database.models import init_db

def main():
    """Initialize the database."""
    # Load environment variables
    load_dotenv()
    
    # Get database URL
    db_url = os.environ['DATABASE_URL']
    
    print("Initializing database...")
    init_db(db_url)
    print("Database initialization complete!")

if __name__ == "__main__":
    main()
