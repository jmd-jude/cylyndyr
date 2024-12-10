"""Query generation and execution components."""
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain.memory import ConversationBufferMemory
import pandas as pd
import yaml
import re
from datetime import datetime
import logging
import os
import snowflake.connector
from dotenv import load_dotenv
import sqlparse
import streamlit as st
from src.database.db_manager import DatabaseManager
from src.utils.formatting import format_dataframe

# Load environment variables
load_dotenv(override=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Always add StreamHandler for console output
    ]
)

# Try to set up file logging, but don't fail if we can't
try:
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    # Add FileHandler if we can create the directory
    file_handler = logging.FileHandler(f'logs/queries_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)
except Exception as e:
    logging.warning(f"Could not set up file logging: {str(e)}")

class QueryGenerator:
    """Handles SQL query generation and execution."""
    
    def __init__(self):
        """Initialize with LLM client and load prompts."""
        self.llm = self._get_llm_client()
        self.memory = ConversationBufferMemory(return_messages=True)
        with open('prompts.yaml', 'r') as file:
            self.prompts = yaml.safe_load(file)['prompts']['sql_generation']
    
    def _get_llm_client(self):
        """Initialize LLM client based on configuration."""
        try:
            model = st.secrets["LLM_MODEL"]
            temperature = float(st.secrets["LLM_TEMPERATURE"])
            
            if "claude" in model.lower():
                return ChatAnthropic(
                    api_key=st.secrets["ANTHROPIC_API_KEY"],
                    model=model,
                    temperature=temperature
                )
            else:
                return ChatOpenAI(
                    api_key=st.secrets["OPENAI_API_KEY"],
                    model=model,
                    temperature=temperature
                )
        except Exception as e:
            logging.error(f"Error initializing LLM client: {str(e)}")
            # Fallback to OpenAI
            return ChatOpenAI(
                api_key=st.secrets["OPENAI_API_KEY"],
                model="gpt-3.5-turbo",
                temperature=0
            )
    
    def _format_chat_history(self, question: str) -> str:
        """Format chat history for context."""
        messages = self.memory.chat_memory.messages
        if not messages:
            return ""
        
        history = []
        for msg in messages[-3:]:  # Last 3 interactions
            if hasattr(msg, 'content') and isinstance(msg.content, str):
                if "SELECT" in msg.content.upper():  # It's a SQL query
                    history.append(f"Previous successful query: {msg.content}")
        
        return "\n".join(history) if history else ""
    
    def _get_snowflake_connection(self):
        """Create Snowflake connection using active connection config."""
        db_manager = DatabaseManager()
        active_conn = db_manager.get_connection(st.session_state.active_connection_id)
        
        if not active_conn:
            raise ValueError("No active connection found")
        
        conn_config = active_conn['config']
        return snowflake.connector.connect(
            account=conn_config['account'],
            user=conn_config['username'],
            password=conn_config['password'],
            database=conn_config['database'],
            warehouse=conn_config['warehouse'],
            schema=conn_config['schema']
        )
    
    def _get_table_list(self, config) -> str:
        """Get simple list of available tables."""
        if not config or 'base_schema' not in config:
            return "CUSTOMER, ORDERS, LINEITEM, PART, PARTSUPP, SUPPLIER, NATION, REGION"
        return ", ".join(config['base_schema']['tables'].keys())
    
    def _get_schema_context(self, config) -> str:
        """Get relevant schema context for queries."""
        if not config or 'base_schema' not in config:
            return ""
        
        context = []
        for table_name, table_info in config['base_schema']['tables'].items():
            table_desc = []
            table_desc.append(f"Table: {table_name}")
            
            # Add fields
            fields = []
            for field_name, field_info in table_info.get('fields', {}).items():
                field_desc = f"- {field_name} ({field_info['type']})"
                if field_info.get('primary_key'):
                    field_desc += " (Primary Key)"
                if field_info.get('foreign_key'):
                    field_desc += f" (Foreign Key -> {field_info['foreign_key']})"
                fields.append(field_desc)
            
            if fields:
                table_desc.append("Fields:\n" + "\n".join(fields))
            context.append("\n".join(table_desc))
        
        return "\n\n".join(context)
    
    def _sanitize_sql(self, query: str) -> str:
        """Clean and format SQL query, ensuring only one statement."""
        # Remove SQL code blocks if present
        query = re.sub(r'```sql|```', '', query)
        
        # Find either WITH or SELECT statement
        match = re.search(r'\b(WITH|SELECT)\b.*', query, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError("No valid SQL query found in the response")
        
        query = match.group(0)
        
        # Remove any text after the last semicolon or after LIMIT clause
        query = re.split(r';|\s+(?=THIS|Let me|Note)', query)[0]
        
        # Format the SQL
        formatted = sqlparse.format(
            query,
            reindent=True,
            keyword_case='upper',
            identifier_case='upper',
            strip_comments=True,
            use_space_around_operators=True
        )
        return formatted.strip()
    
    def generate_query(self, question: str, config=None) -> str:
        """Generate SQL query from natural language question."""
        prompt = self.prompts['template'].format(
            base_role=self.prompts['base_role'].format(database_type="Snowflake"),
            table_list=self._get_table_list(config),
            schema_context=self._get_schema_context(config),
            question=question,
            chat_history=self._format_chat_history(question)
        )
        
        messages = ChatPromptTemplate.from_template(prompt).format_messages(question=question)
        response = self.llm.invoke(messages)
        
        if response and response.content:
            self.memory.chat_memory.add_user_message(question)
            self.memory.chat_memory.add_ai_message(response.content)
        
        return self._sanitize_sql(response.content)
    
    def analyze_result(self, df: pd.DataFrame, question: str, config=None) -> str:
        """Generate schema-aware analysis of query results."""
        analysis_prompt = f"""
        Analyze these query results in the context of the business.
        
        Question Asked: {question}
        
        Data Summary:
        - Row Count: {len(df)}
        - Columns: {', '.join(df.columns)}
        - Numeric Summary: {df.describe().to_string() if not df.empty else 'No numeric data'}
        
        Provide a concise analysis that:
        1. Answers the original question clearly
        2. Highlights key business insights
        3. Notes any significant patterns or anomalies
        
        Keep response under 3-4 sentences and focus on business value.
        """
        
        response = self.llm.invoke([{"role": "user", "content": analysis_prompt}])
        return response.content
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return formatted results."""
        conn = None
        try:
            conn = self._get_snowflake_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            
            return format_dataframe(df)
            
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

# Initialize global query generator
query_generator = QueryGenerator()

def generate_dynamic_query(question: str, config=None) -> str:
    """Generate SQL query from natural language question."""
    return query_generator.generate_query(question, config)

def execute_dynamic_query(query: str, question: str = None) -> pd.DataFrame:
    """Execute generated SQL query and return results."""
    return query_generator.execute_query(query)
