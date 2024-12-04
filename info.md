# Talk with Your Data

An AI-powered natural language to SQL query interface with configurable schema management.

## Features

- Natural language to SQL query conversion
- Support for Snowflake
- Configurable schema documentation
- Chat-like interface for query input
- SQL query visualization and results display
- Configurable AI prompts for query generation

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables in `.env`:
```bash
OPENAI_API_KEY=your_key_here

# For Snowflake (optional):
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_SCHEMA=your_schema
```

1. Ensure you have Snowflake credentials configured in `.env`

## Running the Application

Start the Streamlit app:
```bash
streamlit run app.py
```

Access at http://localhost:8501

## Configuration

### Schema Configuration
Schema configurations are stored in YAML files:
- `schema_configs/snowflake_schema_config.yaml`
- `schema_configs/snowflake_schema_config_v2.yaml`

These files contain:
- Business context
- Table and field descriptions
- Relationships
- Query guidelines

### Prompt Configuration
AI prompts are configured in `prompts.yaml`:
- Base instructions
- Query rules
- Database-specific overrides
- Template structure

## Components

### Data Source Selection
- Automatic validation of database connections
- Dynamic loading of appropriate schema

### Schema Management
- View and edit schema documentation
- Define business context
- Set query guidelines
- Document relationships

### Query Interface
- Natural language query input
- SQL query preview
- Results display
- Error handling and feedback

## Development

### File Structure
```
├── app.py                 # Main Streamlit application
├── prompts.yaml           # AI prompt configuration
├── requirements.txt       # Python dependencies
├── schema_configs/       # Schema documentation
│   ├── snowflake_schema_config.yaml
│   └── snowflake_schema_config_v2.yaml
└── src/
    ├── schema_manager.py
    ├── database/
    └── langchain_components/
```

### Configuration Points

1. Schema Configuration:
- Edit schema_configs/*.yaml for database documentation
- Update business context and descriptions
- Add example queries and guidelines

2. Prompt Engineering:
- Modify prompts.yaml to adjust AI behavior
- Add database-specific rules
- Customize instruction style

3. Feature Flags:
- SHOW_SCHEMA_EDITOR in app.py controls schema editor visibility
- Useful for demos vs development

## Security

- Never commit .env files
- Keep API keys secure
- Use environment variables for credentials
- Review sample.db content before sharing

## Troubleshooting

1. Database Connection:
- Verify database files exist
- Check environment variables
- Confirm database credentials

2. Query Generation:
- Review schema configuration
- Check prompt templates
- Verify OpenAI API key

3. Schema Updates:
- Ensure write permissions on schema_configs/
- Verify YAML syntax
- Check file paths

## Next Steps

1. Enhancements:
- Additional database support
- Advanced query validation
- Schema version control
- Query history management

2. Production Deployment:
- Authentication setup
- Connection security
- Performance optimization
- Error monitoring
