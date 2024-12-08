# Cylyndyr

A natural language interface for querying databases, with support for multiple connections and schema management.

## Setup

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/jmd-jude/cylyndyr.git
cd cylyndyr
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Create a `.env` file with required environment variables:
```
OPENAI_API_KEY=your-key-here
DATABASE_URL=postgresql://...  # Optional: For production database
```

4. Initialize the database:
```bash
python init_db.py
```

5. Run the application:
```bash
streamlit run app.py
```

### Production Deployment

When deploying to Streamlit Cloud, set the following secrets:

- `OPENAI_API_KEY`: Your OpenAI API key
- `DATABASE_URL`: PostgreSQL connection string (if using Supabase or other database)

## Features

- Natural language queries to SQL
- Multiple database connection support
- Schema configuration and management
- Business context for better query understanding
- Support for Snowflake databases

## Development

- Uses SQLite for local development
- PostgreSQL for production
- Streamlit for the user interface
- LangChain for natural language processing
