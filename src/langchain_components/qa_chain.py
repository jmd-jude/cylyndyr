"""Query generation and execution components."""
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
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
        logging.FileHandler(f'logs/queries_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

class QueryGenerator:
    """Handles SQL query generation and execution."""
    
    def __init__(self):
        """Initialize with OpenAI client and load prompts."""
        self.llm = self._get_openai_client()
        with open('prompts.yaml', 'r') as file:
            self.prompts = yaml.safe_load(file)['prompts']['sql_generation']
    
    def _get_openai_client(self):
        """Initialize OpenAI client with API key."""
        api_key = st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key not found")
        return ChatOpenAI(api_key=api_key, model="gpt-3.5-turbo", temperature=0)
    
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
    
    def _is_complex_query(self, question: str) -> bool:
        """Determine if a question requires complex query generation."""
        complex_indicators = [
            'group by', 'join', 'between', 'having',
            'rank', 'over', 'partition', 'compare',
            'trend', 'pattern', 'average', 'total',
            'most', 'least', 'top', 'bottom'
        ]
        return any(indicator in question.lower() for indicator in complex_indicators)
    
    def _get_table_list(self, config) -> str:
        """Get simple list of available tables."""
        if not config or 'base_schema' not in config:
            return "CUSTOMER, ORDERS, LINEITEM, PART, PARTSUPP, SUPPLIER, NATION, REGION"
        return ", ".join(config['base_schema']['tables'].keys())
    
    def _get_schema_context(self, config) -> str:
        """Get relevant schema context for complex queries."""
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
    
    def _get_business_context(self, config) -> str:
        """Extract business context from schema config."""
        if not config or 'business_context' not in config:
            return ""
        
        context = []
        
        # Add business description
        if 'description' in config['business_context']:
            context.append(f"Business Context: {config['business_context']['description']}")
        
        # Add key business concepts
        if 'key_concepts' in config['business_context']:
            concepts = config['business_context']['key_concepts']
            if concepts:
                context.append("Key Business Concepts:")
                context.extend(f"- {concept}" for concept in concepts)
        
        # Add table-specific business context
        if 'table_descriptions' in config['business_context']:
            for table, info in config['business_context']['table_descriptions'].items():
                if 'description' in info:
                    context.append(f"\n{table} represents: {info['description']}")
                if 'fields' in info:
                    for field, field_info in info['fields'].items():
                        if 'description' in field_info:
                            context.append(f"- {field}: {field_info['description']}")
        
        return "\n".join(context)
    
    def _sanitize_sql(self, query: str) -> str:
        """Clean and format SQL query, ensuring only one statement."""
        # Remove SQL code blocks if present
        query = re.sub(r'```sql|```', '', query)
        
        # Parse all statements
        statements = sqlparse.split(query)
        
        # Get the first non-empty statement
        for stmt in statements:
            if stmt.strip():
                # Format the single statement
                formatted = sqlparse.format(
                    stmt,
                    reindent=True,
                    keyword_case='upper',
                    identifier_case='upper',
                    strip_comments=True,
                    use_space_around_operators=True
                )
                return formatted.strip()
        
        raise ValueError("No valid SQL statement found in the response")
    
    def generate_query(self, question: str, config=None) -> str:
        """Generate SQL query from natural language question."""
        is_complex = self._is_complex_query(question)
        prompt_template = self.prompts['complex' if is_complex else 'simple']['template']
        
        # Format prompt with appropriate context
        prompt = prompt_template.format(
            base_role=self.prompts['base_role'].format(database_type="Snowflake"),
            table_list=self._get_table_list(config),
            schema_context=self._get_schema_context(config) if is_complex else "",
            question=question,
            formatted_rules="\n".join([
                "1. Use UPPERCASE for table and column names",
                "2. Use exact column names from the schema",
                "3. Use appropriate JOIN conditions",
                "4. Handle NULL values explicitly"
            ]) if is_complex else ""
        )
        
        messages = ChatPromptTemplate.from_template(prompt).format_messages(question=question)
        response = self.llm.invoke(messages)
        return self._sanitize_sql(response.content)
    
    def analyze_result(self, df: pd.DataFrame, question: str, config=None) -> str:
        """Generate schema-aware analysis of query results."""
        # Get relevant business context
        business_context = self._get_business_context(config)
        
        # Prepare analysis prompt
        analysis_prompt = f"""
        Analyze these query results in the context of the business.
        
        Question Asked: {question}
        
        Data Summary:
        - Row Count: {len(df)}
        - Columns: {', '.join(df.columns)}
        - Numeric Summary: {df.describe().to_string() if not df.empty else 'No numeric data'}
        
        Business Context:
        {business_context}
        
        Provide a concise analysis that:
        1. Answers the original question clearly
        2. Highlights key business insights
        3. Notes any significant patterns or anomalies
        4. Relates findings to business context
        
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
