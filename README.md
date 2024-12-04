# Cylyndyr

A natural language interface for your data.

## Multi-Connection Support

Cylyndyr now supports multiple database connections, allowing you to:
- Connect to multiple Snowflake databases
- Maintain separate schema configurations per connection
- Switch between connections easily

### Setup

1. Initialize the database:
```bash
cd src/database
python init_db.py
```

2. Run the application:
```bash
streamlit run app.py
```

On first run, the app will:
- Create a SQLite database to store connection information
- Migrate your existing Snowflake connection (from environment variables or Streamlit secrets)
- Migrate your existing schema configuration

### Adding New Connections

Once the app is running with multi-connection support enabled:
1. Your existing Snowflake connection will be automatically migrated
2. Additional connections can be managed through the UI
3. Each connection maintains its own schema configuration

### Environment Variables

For local development, set these environment variables or use a `.env` file:

```env
# OpenAI
OPENAI_API_KEY=your_api_key

# Default Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_SCHEMA=your_schema
```

### Streamlit Cloud Deployment

For Streamlit Cloud deployment, add these secrets:

```toml
[openai]
api_key = "your_api_key"

[snowflake]
account = "your_account"
user = "your_user"
password = "your_password"
database = "your_database"
warehouse = "your_warehouse"
schema = "your_schema"
```

## Features

- Natural language queries to SQL
- Schema-aware query generation
- Customizable schema configurations
- Multiple database connections
- Result analysis and formatting
- Chat history
- Schema configuration editor

## Architecture

The application uses:
- Streamlit for the user interface
- LangChain for natural language processing
- SQLAlchemy for database management
- SQLite for storing connection and configuration data
- Snowflake for data warehousing

## Development

To contribute or modify:

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Set up environment variables
4. Initialize the database
5. Run the application

## Notes

- The application maintains backward compatibility with single-connection mode
- Schema configurations are stored per-connection in the SQLite database
- Connection credentials are stored securely in the SQLite database
- Each user gets their own set of connections and configurations
