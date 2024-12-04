from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
import pandas as pd
import yaml
import re
from collections import defaultdict
from datetime import datetime
import logging
import os
import json
import snowflake.connector
from dotenv import load_dotenv
import sqlparse

# Load environment variables
load_dotenv(override=True)

# Set up logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f'chat_history_{datetime.now().strftime("%Y%m%d")}.log')),
        logging.StreamHandler()
    ]
)

def get_openai_client():
    """Initialize OpenAI client with API key."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OpenAI API key not found in environment variables")
    
    logging.info(f"API Key found: {bool(api_key)}")
    logging.info(f"API Key length: {len(api_key)}")
    logging.info(f"API Key prefix: {api_key[:7]}...")
    
    return ChatOpenAI(
        api_key=api_key,
        model="gpt-3.5-turbo",
        temperature=0
    )

def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

class QueryMemoryManager:
    """Manages query history and interactions."""
    
    def __init__(self, window_size=7):
        self.history = defaultdict(list)
        self.window_size = window_size
    
    def get_chat_history(self, thread_id: str = "default"):
        return self.history[thread_id][-self.window_size:] if self.history[thread_id] else []
    
    def format_result(self, result):
        """Format result for storage, with special handling for DataFrames."""
        if isinstance(result, pd.DataFrame):
            if result.empty:
                return "Empty DataFrame"
            # Format DataFrame with clean spacing and alignment
            return result.to_string(
                index=False,
                max_rows=10,  # Limit rows for readability
                max_cols=None,
                line_width=80,
                justify='left'
            )
        return str(result)
    
    def save_interaction(self, thread_id: str, question: str, query: str, result):
        """Save interaction with improved result formatting."""
        interaction = {
            'timestamp': datetime.now().isoformat(),
            'thread_id': thread_id,
            'question': question,
            'query': query,
            'result': self.format_result(result)
        }
        self.history[thread_id].append(interaction)
        logging.info(json.dumps(interaction, indent=2))

def load_prompt_config():
    """Load prompt configuration from YAML."""
    try:
        with open('prompts.yaml', 'r') as file:
            config = yaml.safe_load(file)
            return config['prompts']['sql_generation']
    except Exception as e:
        logging.error(f"Error loading prompt config: {str(e)}")
        raise

def format_schema_context(config):
    """Convert schema config into prompt-friendly format."""
    if not config:
        return ""
        
    context = [f"Business Context: {config['business_context']['description']}"]
    
    concepts = "\n".join(f"- {c}" for c in config['business_context']['key_concepts'])
    context.append(f"Key Business Concepts:\n{concepts}")
    
    for table_name, table_info in config['tables'].items():
        context.append(f"\nTable: {table_name}")
        context.append(f"Description: {table_info['description']}")
        
        fields = []
        for field_name, field_info in table_info['fields'].items():
            field_desc = f"- {field_name} ({field_info['type']}): {field_info['description']}"
            if field_info.get('is_key'):
                field_desc += " (Primary Key)"
            if field_info.get('foreign_key'):
                field_desc += f" (Foreign Key -> {field_info['foreign_key']})"
            fields.append(field_desc)
        context.append("Fields:\n" + "\n".join(fields))
        
        if 'relationships' in table_info:
            rels = []
            for rel in table_info['relationships']:
                rels.append(f"- {rel['type']} relationship with {rel['table']} on {rel['join_fields']}")
            context.append("Relationships:\n" + "\n".join(rels))
    
    if config.get('query_guidelines'):
        tips = "\n".join(f"- {tip}" for tip in config['query_guidelines']['tips'])
        context.append(f"\nQuery Guidelines:\n{tips}")
    
    return "\n\n".join(context)

def format_example_queries(config):
    """Format example queries from the configuration."""
    if not config:
        return ""
        
    db_config = config.get('database_config', {})
    if not db_config or 'example_queries' not in db_config:
        return ""

    examples = ["\nExample Queries:"]
    for example in db_config['example_queries']:
        examples.append(f"\n{example['description']}:")
        examples.append(example['query'].strip())
    
    return "\n".join(examples)

def sanitize_sql(query):
    """Sanitize SQL query using sqlparse for better reliability."""
    # Remove SQL code blocks if present
    query = re.sub(r'```sql|```', '', query)
    
    # Format the SQL properly using sqlparse
    formatted = sqlparse.format(
        query,
        reindent=True,
        keyword_case='upper',
        identifier_case='upper',  # Snowflake uses uppercase
        strip_comments=True,
        use_space_around_operators=True
    )
    
    return formatted.strip()

def get_data_timeframe():
    """Get the actual timeframe of data in the ORDERS table."""
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                MIN(O_ORDERDATE) as min_date,
                MAX(O_ORDERDATE) as max_date
            FROM ORDERS
        """)
        min_date, max_date = cursor.fetchone()
        return min_date, max_date
    except Exception as e:
        logging.error(f"Error getting data timeframe: {str(e)}")
        return None, None
    finally:
        if conn:
            conn.close()

def create_sql_generation_prompt(chat_history=None, config=None):
    """Create prompt template using configuration from YAML."""
    if not config:
        # Basic template without any schema context or helpful rules
        prompt_text = """
        Generate a SQL query to answer this question. The database contains tables about customers, orders, and products.
        
        Question: {question}
        
        Return only the SQL query without any explanation.
        """
        return ChatPromptTemplate.from_template(prompt_text)
    
    # Full featured template with all context and rules
    prompt_config = load_prompt_config()
    min_date, max_date = get_data_timeframe()
    
    if 'query_rules' not in prompt_config:
        prompt_config['query_rules'] = []
    
    prompt_config['query_rules'].extend([
        "Use UPPERCASE for table and column names",
        "Table names in TPC-H are: CUSTOMER, ORDERS, LINEITEM, PART, PARTSUPP, SUPPLIER, NATION, REGION",
        "Always use the exact column names from the schema (e.g., C_CUSTKEY, O_ORDERKEY)",
        "Use Snowflake date functions (e.g., DATE_TRUNC, DATE_PART) for date operations",
        f"Data timeframe: Orders from {min_date} to {max_date}",
        "If a query returns empty results, try to determine why and adjust the query accordingly"
    ])
    
    # Format contexts
    formatted_rules = "\n".join(f"{i+1}. {rule}" for i, rule in enumerate(prompt_config['query_rules']))
    schema_context = format_schema_context(config)
    example_queries = format_example_queries(config)
    
    # Format history context with emphasis on empty results
    history_context = ""
    if chat_history:
        history_entries = []
        for h in chat_history:
            entry = f"Q: {h['question']}\nSQL: {h['query']}"
            if "Empty DataFrame" in h['result']:
                entry += "\nResult: EMPTY RESULT - Query needs adjustment"
            else:
                entry += f"\nResult: {h['result']}"
            history_entries.append(entry)
        history_context = f"\nRecent Query History:\n" + "\n\n".join(history_entries) + "\n"
    
    # Full template with schema context
    prompt_text = prompt_config['template'].format(
        base_role=prompt_config['base_role'].format(database_type="Snowflake"),
        main_instruction=prompt_config['main_instruction'],
        schema_context=schema_context,
        example_queries=example_queries,
        history_context=history_context,
        formatted_rules=formatted_rules,
        question="{question}"  # Left for ChatPromptTemplate to fill
    )
    
    return ChatPromptTemplate.from_template(prompt_text)

def refine_query_if_empty(question: str, original_query: str, thread_id: str = "default"):
    """Generate a refined query if the original returns empty results."""
    refinement_prompt = f"""
    The following query returned no results:
    {original_query}
    
    This query was generated for the question: "{question}"
    
    Please analyze why this might have returned no results and generate a modified query that:
    1. Considers the actual timeframe of the data
    2. Relaxes or adjusts any overly restrictive conditions
    3. Maintains the original intent of the question
    
    Generate only the SQL query, no explanation needed.
    """
    
    llm = get_openai_client()
    response = llm.invoke([{"role": "user", "content": refinement_prompt}])
    return sanitize_sql(response.content)

memory_manager = QueryMemoryManager()

def generate_dynamic_query(question: str, thread_id: str = "default", config=None):
    """Generate SQL query from natural language question."""
    try:
        llm = get_openai_client()
        chat_history = memory_manager.get_chat_history(thread_id)
        prompt = create_sql_generation_prompt(chat_history, config)
        
        messages = prompt.format_messages(question=question)
        response = llm.invoke(messages)
        return sanitize_sql(response.content)
    except Exception as e:
        logging.error(f"Error generating query: {str(e)}")
        raise

def execute_dynamic_query(query: str, question: str = None, thread_id: str = "default"):
    """Execute generated SQL query and return results."""
    conn = None
    try:
        # Create a new connection for each query
        conn = get_snowflake_connection()
        
        # Execute query and fetch results into pandas DataFrame
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # If results are empty, try to refine the query
        if df.empty and question:
            refined_query = refine_query_if_empty(question, query, thread_id)
            if refined_query != query:  # Only if we got a different query
                cursor.execute(refined_query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                query = refined_query  # Update query to the refined version
        
        if question:
            memory_manager.save_interaction(thread_id, question, query, df)
        return df
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        if question:
            memory_manager.save_interaction(thread_id, question, query, error_msg)
        return error_msg
    finally:
        if conn:
            conn.close()
