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

## Usage Tips

### Handling Query Errors

If you encounter an error when running a query, don't worry! The system has built-in error recovery:

1. Simply ask it to try again with a message like:
   - "Can you try that again?"
   - "I got an error, can you fix it?"
   - "That didn't work, please retry"

2. The system will:
   - Analyze the specific error
   - Adjust the query accordingly
   - Try again with the corrected version

This retry capability is particularly helpful when:
- Column names need adjustment
- Table relationships need clarification
- Data type conversions are required

Most query issues can be resolved with a simple retry request, making the system more user-friendly for prototype testing.

## Development

- Uses SQLite for local development
- PostgreSQL for production
- Streamlit for the user interface
- LangChain for natural language processing
