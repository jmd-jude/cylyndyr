import os
from dotenv import load_dotenv
from src.langchain_components.qa_chain import generate_dynamic_query, execute_dynamic_query
from src.database.schema_inspector import inspect_database, save_schema_config
import pandas as pd

def test_sqlite_query():
    """Test querying SQLite database"""
    question = "What are the total sales by product category?"
    query = generate_dynamic_query(question, db_type="sqlite")
    print("\nSQLite Query:")
    print(query)
    
    results = execute_dynamic_query(query, question, db_type="sqlite")
    if isinstance(results, pd.DataFrame):
        print("\nResults:")
        print(results)
    else:
        print("\nError:", results)

def test_snowflake_query():
    """Test querying Snowflake database"""
    # First, generate schema config if it doesn't exist
    if not os.path.exists('snowflake_schema_config.yaml'):
        print("\nGenerating Snowflake schema configuration...")
        config = inspect_database(
            db_type="snowflake",
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        save_schema_config(config, 'snowflake_schema_config.yaml')
        print("Schema configuration saved to snowflake_schema_config.yaml")
    
    # Test a query
    question = "Show me the first 5 rows of any table in the database"
    query = generate_dynamic_query(question, db_type="snowflake")
    print("\nSnowflake Query:")
    print(query)
    
    results = execute_dynamic_query(query, question, db_type="snowflake")
    if isinstance(results, pd.DataFrame):
        print("\nResults:")
        print(results)
    else:
        print("\nError:", results)

def main():
    # Load environment variables
    load_dotenv()
    
    # Check if we have Snowflake credentials
    has_snowflake = all(os.getenv(var) for var in [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_SCHEMA'
    ])
    
    print("Testing SQLite queries...")
    test_sqlite_query()
    
    if has_snowflake:
        print("\nTesting Snowflake queries...")
        test_snowflake_query()
    else:
        print("\nSkipping Snowflake tests - credentials not found in .env file")
        print("Required environment variables:")
        print("  SNOWFLAKE_ACCOUNT")
        print("  SNOWFLAKE_USER")
        print("  SNOWFLAKE_PASSWORD")
        print("  SNOWFLAKE_DATABASE")
        print("  SNOWFLAKE_WAREHOUSE")
        print("  SNOWFLAKE_SCHEMA")

if __name__ == "__main__":
    main()
