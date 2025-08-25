"""Query generation and execution components."""
from src.llm.client import LLMClient
import pandas as pd
import yaml
import re
from datetime import datetime, date
import time
import logging
import os
import snowflake.connector
from dotenv import load_dotenv
import sqlparse
import streamlit as st
from src.database.db_manager import DatabaseManager
from src.utils.formatting import format_dataframe
from uuid import uuid4
import json
from decimal import Decimal

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

def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

class QueryGenerator:
    """Handles SQL query generation and execution."""

    def __init__(self):
        """Initialize with LLM client and load prompts."""
        self.llm = LLMClient()
        self.thread_id = str(uuid4())
        self.last_error = None  # Track the last query error
        with open('prompts.yaml', 'r') as file:
            self.prompts = yaml.safe_load(file)['prompts']['sql_generation']

        # Set up structured logging
        self.logger = logging.getLogger('qa_chain')
        self.logger.setLevel(logging.INFO)
        # Add JSON formatter for structured logging
        json_handler = logging.FileHandler(f'logs/qa_chain_{datetime.now().strftime("%Y%m%d")}.json')
        json_handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(json_handler)

    def _log_interaction(self, interaction_type: str, **kwargs):
        """Log structured interaction data to both file and Supabase."""
        current_time = datetime.now()
        
        # Process kwargs for JSON compatibility
        def json_safe_value(v):
            if isinstance(v, (datetime, date)):
                return v.isoformat()
            if isinstance(v, Decimal):
                return float(v)
            return v

        def process_payload(obj):
            if isinstance(obj, dict):
                return {k: process_payload(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [process_payload(i) for i in obj]
            return json_safe_value(obj)

        processed_kwargs = process_payload(kwargs)
        log_entry = {
            'timestamp': current_time.isoformat(),
            'thread_id': self.thread_id,
            'type': interaction_type,
            'database_name': st.session_state.get('active_connection_name'),
            'user_id': st.session_state.get('user_id'),
            **processed_kwargs
        }

        # Keep existing file logging
        self.logger.info(json.dumps(log_entry))

        # NEW: Log to Supabase regardless of user's connection
        try:
            from src.database.db_manager import DatabaseManager
            db_manager = DatabaseManager()
            
            success = db_manager.save_interaction_log(
                user_id=st.session_state.get('user_id'),
                connection_id=st.session_state.get('active_connection_id'),
                thread_id=self.thread_id,
                interaction_type=interaction_type,
                database_name=st.session_state.get('active_connection_name'),
                payload=processed_kwargs
            )
            
            if not success:
                logging.error(f"Error saving interaction log: {str(e)}")
                
        except Exception as e:
            # Don't let logging break the main flow
            logging.warning(f"Failed to log interaction to Supabase: {str(e)}")

    def _format_chat_history(self, question: str) -> str:
        """Format chat history using Streamlit session state."""
        if 'chat_history' not in st.session_state or not st.session_state.chat_history:
            return ""
        
        history = []
        # Get last 3 interactions (reduced from 7 for better performance)
        recent_history = st.session_state.chat_history[-3:]
        
        for interaction in recent_history:
            if 'query' in interaction:
                history.append(f"Previous query: {interaction['query']}")
        
        # Add error context if available
        if self.last_error:
            history.append(f"Previous error: {self.last_error}")
        
        return "\n".join(history)

    def _format_analysis_history(self) -> str:
        """Format analysis conversation history using session state."""
        if 'chat_history' not in st.session_state or not st.session_state.chat_history:
            return ""
        
        history = []
        # Get last 4 interactions for analysis context
        recent_history = st.session_state.chat_history[-4:]
        
        for interaction in recent_history:
            if interaction.get('type') == 'analysis':
                history.append(f"Previous analysis: {interaction['result'][:200]}...")
            elif 'question' in interaction:
                history.append(f"User question: {interaction['question']}")
        
        return "\n".join(history)

    def _get_snowflake_connection(self):
        """Create Snowflake connection using private key authentication only."""
        db_manager = DatabaseManager()
        active_conn = db_manager.get_connection(st.session_state.active_connection_id)
        
        if not active_conn:
            raise ValueError("No active connection found")
        
        conn_config = active_conn['config']
        
        # Base connection parameters
        connection_params = {
            'account': conn_config['account'],
            'user': conn_config['username'],
            'database': conn_config['database'],
            'warehouse': conn_config['warehouse'],
            'schema': conn_config['schema']
        }
        
        # Private key authentication only
        private_key_path = conn_config.get('private_key_path')
        if not private_key_path:
            raise ValueError("Private key path not specified in config")
        
        try:
            # Try to get private key from environment or secrets
            private_key_content = os.getenv(private_key_path)
            
            # If not found, try streamlit secrets
            if not private_key_content:
                try:
                    private_key_content = st.secrets[private_key_path]
                except Exception:
                    pass
            
            # If still not found, treat as file path
            if not private_key_content:
                if os.path.exists(private_key_path):
                    with open(private_key_path, "rb") as key_file:
                        private_key_content = key_file.read()
                else:
                    raise ValueError(f"Private key not found: {private_key_path}")
            
            # If it's a string (from env/secrets), process it carefully
            if isinstance(private_key_content, str):
                # Handle potential newline issues in environment variables
                private_key_content = private_key_content.replace('\\n', '\n')
                private_key_content = private_key_content.encode('utf-8')
            
            # Parse the private key using cryptography library
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            
            # Load the PEM private key
            private_key_obj = load_pem_private_key(
                private_key_content,
                password=None  # We generated unencrypted key
            )
            
            # Convert to DER format (binary) that Snowflake expects
            private_key_der = private_key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            connection_params['private_key'] = private_key_der
            
        except Exception as e:
            raise ValueError(f"Failed to load private key: {str(e)}")
        
        return snowflake.connector.connect(**connection_params)

    def _get_table_list(self, config) -> str:
        """Get list of available tables (v2.0 format only)."""
        if not config:
            return ""
        return ", ".join(config.get('tables', {}).keys())

    def _get_schema_context(self, config) -> str:
        """Get relevant schema context for queries (v2.0 format only)."""
        if not config:
            return ""
            
        context_parts = []
        
        # Handle business context
        business_context = config.get('business_context', {})
        if business_context.get('description'):
            context_parts.append("Business Context:\n" + business_context['description'])
        if business_context.get('key_concepts'):
            context_parts.append("Key Business Concepts:\n- " + "\n- ".join(business_context['key_concepts']))
        
        # Handle query guidelines
        query_guidelines = config.get('query_guidelines', {})
        if query_guidelines.get('optimization_rules'):
            context_parts.append("Query Guidelines:\n- " + "\n- ".join(query_guidelines['optimization_rules']))
        
        # Handle tables (v2.0 format only)
        tables = config.get('tables', {})
        for table_name, table_info in tables.items():
            table_parts = [f"Table: {table_name}"]
            
            # Add table description if available
            if table_info.get('description'):
                table_parts.append(f"Description: {table_info['description']}")
            
            # Process fields efficiently
            fields = []
            for field_name, field_info in table_info.get('fields', {}).items():
                field_desc = [f"- {field_name} ({field_info.get('type', 'TEXT')})"]
                
                # Add attributes
                attributes = []
                if field_info.get('primary_key'):
                    attributes.append("Primary Key")
                if field_info.get('foreign_key'):
                    attributes.append(f"Foreign Key -> {field_info['foreign_key']}")
                if field_info.get('nullable'):
                    attributes.append("Optional")
                if attributes:
                    field_desc.append(f"  ({', '.join(attributes)})")
                
                # Add business description
                if field_info.get('description'):
                    field_desc.append(f"  Description: {field_info['description']}")
                
                fields.append(" ".join(field_desc))
            
            if fields:
                table_parts.append("Fields:\n" + "\n".join(fields))
            
            context_parts.append("\n".join(table_parts))
        
        return "\n\n".join(context_parts)

    def _get_business_context(self, config) -> str:
        """Extract business context from config."""
        if not config or not config.get('business_context'):
            return ""
        context = ""
        if config['business_context'].get('description'):
            context += f"\nBusiness Context: {config['business_context']['description']}"
        if config['business_context'].get('key_concepts'):
            context += f"\nKey Concepts: {', '.join(config['business_context']['key_concepts'])}"
        return context

    def _get_field_context(self, df: pd.DataFrame, config) -> str:
        """Extract field context for current columns (v2.0 format only)."""
        if not config:
            return ""
            
        field_descriptions = []
        tables = config.get('tables', {})
        
        for table_info in tables.values():
            for field_name, field_info in table_info.get('fields', {}).items():
                if field_name in df.columns and field_info.get('description'):
                    field_descriptions.append(f"{field_name}: {field_info['description']}")
        
        return "\nField Descriptions:\n- " + "\n- ".join(field_descriptions) if field_descriptions else ""

    def _sanitize_sql(self, query: str) -> str:
        """Clean and format SQL query, ensuring only one statement."""
        query = re.sub(r'```sql|```', '', query)
        match = re.search(r'\b(WITH|SELECT)\b.*', query, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError("No valid SQL query found in the response")
        query = match.group(0)
        query = re.split(r';|\s+(?=THIS|Let me|Note)', query)[0]
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
        response_content = self.llm.generate(prompt)
        generated_sql = self._sanitize_sql(response_content)
        # Log query generation
        self._log_interaction(
            'query_generation',
            user_question=question,
            generated_sql=generated_sql,
            prompt_used=prompt
        )
    
        return generated_sql

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return formatted results."""
        conn = None
        start_time = time.time()
        
        try:
            conn = self._get_snowflake_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            formatted_df = format_dataframe(df)
            
            # Calculate execution time
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Clear last error on successful query
            self.last_error = None
            
            # Log query execution with results
            self._log_interaction(
                'query_execution',
                sql_query=query,
                row_count=len(df),
                column_count=len(df.columns),
                columns=columns,
                results_sample=formatted_df.head(5).to_dict('records'),
                numeric_summary=formatted_df.describe().to_dict() if not df.empty else None,
                execution_time_ms=execution_time_ms
            )
            
            # Save to query history if we have session context
            self._save_to_query_history(query, formatted_df, execution_time_ms)
            
            return formatted_df
            
        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            # Store the error for context in future queries
            self.last_error = str(e)
            # Log query execution error
            self._log_interaction(
                'query_error',
                sql_query=query,
                error=str(e),
                execution_time_ms=execution_time_ms
            )
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _save_to_query_history(self, query: str, result_df: pd.DataFrame, execution_time_ms: int):
        """Save successful query to user's history."""
        try:
            # Only save if we have active session context
            if (hasattr(st.session_state, 'user_id') and st.session_state.user_id and
                hasattr(st.session_state, 'active_connection_id') and st.session_state.active_connection_id and
                hasattr(st.session_state, 'current_question') and st.session_state.current_question):
                
                from src.database.db_manager import DatabaseManager
                db_manager = DatabaseManager()
                
                db_manager.save_query_to_history(
                    user_id=st.session_state.user_id,
                    connection_id=st.session_state.active_connection_id,
                    question=st.session_state.current_question,
                    generated_sql=query,
                    result_df=result_df,
                    execution_time_ms=execution_time_ms
                )
        except Exception as e:
            # Don't let history saving break the main query flow
            logging.warning(f"Failed to save query to history: {str(e)}")

    def analyze_result(self, df: pd.DataFrame, original_question: str, config=None) -> str:
        """Generate schema-aware analysis of query results."""
        business_context = self._get_business_context(config)
        field_context = self._get_field_context(df, config)
        # Prepare data context based on size
        if len(df) <= 100:
            # For small result sets, include all data
            data_context = f"""
            Full Dataset ({len(df)} rows):
            {df.to_dict('records')}
            """
        else:
            # For larger sets, take a smart sample
            sample_size = min(100, len(df) // 10)
            sampled_df = df.sample(n=sample_size, random_state=42)  # Fixed random state for reproducibility
            data_context = f"""
            Sample Dataset ({sample_size} rows from total {len(df)}):
            {sampled_df.to_dict('records')}
            Value Ranges:
            {df.describe().to_dict()}
            """
        analysis_prompt = f"""
        You are a skilled SQL analyst explaining your query approach to a business user. They asked a question in plain English, and you want to help them understand how the resulting dataset answers their question.
        CONTEXT:
        Original Question: {original_question}{business_context}{field_context}
        {data_context}
        EXPLANATION FRAMEWORK:
        1. Query Overview
        - Start with a plain-English summary of how you approached answering their question
        - Explain why you chose to structure the data the way you did
        - Highlight any clever or non-obvious ways you transformed the data to match their needs
        2. Data Pipeline Walkthrough
        - Break down the major steps in how the data was assembled
        - Explain any important data joins or combinations and why they were necessary
        - Note any filtering, grouping, or aggregations that shaped the final result
        - Describe any calculated fields and what they represent
        3. Result Structure
        - Explain what each column in the result represents in business terms
        - Clarify any potentially confusing aspects of how the data is organized
        - Note if any data was deliberately excluded and why
        - Highlight any assumptions made in how the data was structured
        4. Data Quality Context
        - Note any important caveats about the data (e.g., time periods covered, excluded scenarios)
        - Explain any null values or special cases in the results
        - Mention any data transformations that might affect interpretation
        FORMATTING REQUIREMENTS:
        - Use plain English, avoiding technical SQL terms unless necessary
        - When technical terms are needed, explain them in business context
        - Format numbers with commas for thousands (e.g., "1,234" not "1234")
        - Express percentages as "X%" (e.g., "28%" not "28 percent")
        Keep explanations focused on helping the user understand how their question was translated into data operations. Avoid analyzing the business implications - that will come later in the discussion mode. End with "Toggle to 'Discussion Mode' to explore what these results mean for your business."
        """
        response = self.llm.generate(analysis_prompt)
        # Log analysis
        self._log_interaction(
            'analysis',
            original_question=original_question,
            analysis_response=response,
            data_shape={'rows': len(df), 'columns': len(df.columns)}
        )
        
        return response

    def continue_analysis(self, follow_up: str, df: pd.DataFrame, original_question: str, config=None) -> str:
        """Continue analysis conversation about the results."""
        business_context = self._get_business_context(config)
        field_context = self._get_field_context(df, config)
        conversation_history = self._format_analysis_history()
        # Prepare data context based on size (same as analyze_result)
        if len(df) <= 100:
            # For small result sets, include all data
            data_context = f"""
            Full Dataset ({len(df)} rows):
            {df.to_dict('records')}
            """
        else:
            # For larger sets, take a smart sample
            sample_size = min(100, len(df) // 10)
            sampled_df = df.sample(n=sample_size, random_state=42)  # Fixed random state for reproducibility
            data_context = f"""
            Sample Dataset ({sample_size} rows from total {len(df)}):
            {sampled_df.to_dict('records')}
            Value Ranges:
            {df.describe().to_dict()}
            """
        analysis_prompt = f"""
        You are a Business Intelligence Advisor engaged in an ongoing data exploration.
        FORMATTING REQUIREMENTS:
        - Use plain text only - no special characters or mathematical symbols
        - Maintain proper spacing between all words and numbers
        - Format numbers with commas for thousands (e.g., "1,234" not "1234")
        - Use "to" instead of dashes or other symbols for ranges
        - Express percentages as "X%" (e.g., "28%" not "28 percent")
        - Keep all words separate (e.g., "significantly above the mean" not "significantlyabovethemean")
        CONVERSATION CONTEXT:
        Original Question: {original_question}
        Current Follow-up: {follow_up}
        Previous Discussion:
        {conversation_history}
        BUSINESS CONTEXT:
        {business_context}{field_context}
        {data_context}
        RESPONSE APPROACH:
        0. Context Integration
        - Connect current findings with previous observations
        - Highlight emerging patterns across analyses
        - Note any shifts in understanding
        1. Direct Answer
        - Address the specific follow-up question
        - Connect to previous insights
        - Highlight new findings
        2. Deeper Investigation
        - Explore underlying factors
        - Challenge assumptions
        - Identify correlations
        - Present an unexpected or non-obvious angle
        3. Next Steps
        - Suggest additional angles to explore
        - Identify data gaps if any
        - Recommend concrete actions
        """
        response = self.llm.generate(analysis_prompt)
        # Log follow-up analysis
        self._log_interaction(
            'follow_up_analysis',
            original_question=original_question,
            follow_up_question=follow_up,
            analysis_response=response,
            data_shape={'rows': len(df), 'columns': len(df.columns)}
        )

        return response

# Initialize query generator
def get_query_generator():
    if 'query_generator' not in st.session_state:
        st.session_state.query_generator = QueryGenerator()
    return st.session_state.query_generator

def generate_dynamic_query(question: str, config=None) -> str:
    return get_query_generator().generate_query(question, config)

def execute_dynamic_query(query: str, question: str = None) -> pd.DataFrame:
    return get_query_generator().execute_query(query)