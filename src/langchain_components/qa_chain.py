"""Query generation and execution components."""
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain.memory import ConversationBufferMemory
import pandas as pd
import yaml
import re
from datetime import datetime, date
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
        self.llm = self._get_llm_client()
        self.thread_id = str(uuid4())  # Generate unique thread ID for each instance
        self.memory = ConversationBufferMemory(return_messages=True)
        self.analysis_memory = ConversationBufferMemory(return_messages=True)
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
        """Log structured interaction data to file and database."""
        current_time = datetime.now()
        
        # Pre-process the kwargs to handle special types
        def json_safe_value(v):
            if isinstance(v, (datetime, date)):
                return v.isoformat()
            if isinstance(v, Decimal):
                return float(v)
            return v
        
        # Recursively process nested dictionaries and lists
        def process_payload(obj):
            if isinstance(obj, dict):
                return {k: process_payload(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [process_payload(i) for i in obj]
            return json_safe_value(obj)
        
        # Process the kwargs
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
        
        # Add database logging
        conn = None
        try:
            conn = self._get_snowflake_connection()
            cursor = conn.cursor()
            
            # Convert to object that Snowflake can handle
            snowflake_payload = json.dumps(log_entry)
            
            cursor.execute(
                """
                INSERT INTO APP_METRICS.PUBLIC.LOG_INTERACTIONS 
                (TIMESTAMP, THREAD_ID, TYPE, DATABASE_NAME, USER_ID, PAYLOAD, CREATED_AT) 
                SELECT 
                    %s, %s, %s, %s, %s, 
                    PARSE_JSON(%s),
                    %s
                """,
                (
                    current_time,
                    self.thread_id,
                    interaction_type,
                    st.session_state.get('active_connection_name'),
                    st.session_state.get('user_id'),
                    snowflake_payload,
                    current_time
                )
            )
            conn.commit()
        except Exception as e:
            logging.error(f"Failed to log to database: {str(e)}")
            logging.error(f"Attempted payload: {snowflake_payload}")  # Add this for debugging
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    
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
        for msg in messages[-7:]:  # Last 7 interactions
            if hasattr(msg, 'content') and isinstance(msg.content, str):
                if "SELECT" in msg.content.upper():  # It's a SQL query
                    history.append(f"Previous successful query: {msg.content}")
        
        return "\n".join(history) if history else ""
    
    def _format_analysis_history(self) -> str:
        """Format analysis conversation history."""
        messages = self.analysis_memory.chat_memory.messages
        if not messages:
            return ""
        
        history = []
        for msg in messages[-6:]:  # Last 6 interactions
            if hasattr(msg, 'content'):
                role = "User" if msg.type == "human" else "Assistant"
                history.append(f"{role}: {msg.content}")
        
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
        
        context_parts = []
        if config.get('business_context', {}).get('description'):
            context_parts.append("Business Context:\n" + config['business_context']['description'])
        
        if config['business_context'].get('key_concepts'):
            context_parts.append("Key Business Concepts:\n- " + "\n- ".join(config['business_context']['key_concepts']))
        
        if config.get('query_guidelines', {}).get('optimization_rules'):
            context_parts.append("Query Guidelines:\n- " + "\n- ".join(config['query_guidelines']['optimization_rules']))
        
        for table_name, table_info in config['base_schema']['tables'].items():
            table_parts = [f"Table: {table_name}"]
            
            table_desc = config.get('business_context', {}).get('table_descriptions', {}).get(table_name, {}).get('description')
            if table_desc:
                table_parts.append(f"Description: {table_desc}")
            
            fields = []
            for field_name, field_info in table_info.get('fields', {}).items():
                field_desc = [f"- {field_name} ({field_info['type']})"]
                
                attributes = []
                if field_info.get('primary_key'):
                    attributes.append("Primary Key")
                if field_info.get('foreign_key'):
                    attributes.append(f"Foreign Key -> {field_info['foreign_key']}")
                if field_info.get('nullable'):
                    attributes.append("Optional")
                if attributes:
                    field_desc.append(f"  ({', '.join(attributes)})")
                
                business_desc = (
                    config.get('business_context', {})
                    .get('table_descriptions', {})
                    .get(table_name, {})
                    .get('fields', {})
                    .get(field_name, {})
                    .get('description')
                )
                if business_desc:
                    field_desc.append(f"  Description: {business_desc}")
                
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
        """Extract field context for current columns."""
        if not config or not config.get('base_schema'):
            return ""
        
        field_descriptions = []
        for table_info in config['base_schema']['tables'].values():
            for field_name, field_info in table_info.get('fields', {}).items():
                if field_name in df.columns:
                    desc = (
                        config.get('business_context', {})
                        .get('table_descriptions', {})
                        .get(table_info.get('name', ''), {})
                        .get('fields', {})
                        .get(field_name, {})
                        .get('description')
                    )
                    if desc:
                        field_descriptions.append(f"{field_name}: {desc}")
        
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
        
        messages = ChatPromptTemplate.from_template(prompt).format_messages(question=question)
        response = self.llm.invoke(messages)
        generated_sql = self._sanitize_sql(response.content)
        
        # Log query generation
        self._log_interaction(
            'query_generation',
            user_question=question,
            generated_sql=generated_sql,
            prompt_used=prompt
        )
        
        if response and response.content:
            self.memory.chat_memory.add_user_message(question)
            self.memory.chat_memory.add_ai_message(response.content)
        
        return generated_sql
    
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
            formatted_df = format_dataframe(df)
            
            # Log query execution with results
            self._log_interaction(
                'query_execution',
                sql_query=query,
                row_count=len(df),
                column_count=len(df.columns),
                columns=columns,
                results_sample=formatted_df.head(5).to_dict('records'),
                numeric_summary=formatted_df.describe().to_dict() if not df.empty else None
            )
            
            return formatted_df
            
        except Exception as e:
            # Log query execution error
            self._log_interaction(
                'query_error',
                sql_query=query,
                error=str(e)
            )
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    
    def analyze_result(self, df: pd.DataFrame, question: str, config=None) -> str:
        """Generate schema-aware analysis of query results."""
        business_context = self._get_business_context(config)
        field_context = self._get_field_context(df, config)
        
        analysis_prompt = f"""
        You are a Business Strategy Advisor translating data into actionable insights.

        FORMATTING REQUIREMENTS:
        - Use plain text only - no special characters or mathematical symbols
        - Maintain proper spacing between all words and numbers
        - Format numbers with commas for thousands (e.g., "1,234" not "1234")
        - Use "to" instead of dashes or other symbols for ranges
        - Express percentages as "X%" (e.g., "28%" not "28 percent")
        - Keep all words separate (e.g., "significantly above the mean" not "significantlyabovethemean")

        DATA VALIDATION STEP:
        1. Review row count and structure
        2. Note actual ranges for key metrics
        3. Verify segment sizes and proportions
        4. Cross-check any derived calculations

        Original Question: {question}{business_context}{field_context}

        Data Summary:
        - Row Count: {len(df)}
        - Columns: {', '.join(df.columns)}
        - Numeric Summary: {df.describe().to_string() if not df.empty else 'No numeric data'}

        ANALYSIS FRAMEWORK:

        1. Pattern Recognition
        - Identify significant metric differences between segments/groups
        - Note any surprising similarities or lack of variation
        - Flag counter-intuitive findings
        - Reference specific values from the data to support each observation

        2. Key Findings (in order of business impact)
        - Lead with the most statistically significant insight
        - Quantify using actual values from the data
        - Compare against relevant benchmarks
        - Note where expected patterns are absent

        3. Business Implications
        - Revenue/cost impact (with supporting calculations)
        - Customer/market implications
        - Operational considerations
        - Risk factors and limitations of the analysis

        4. Recommendations
        - Primary action supported by the data
        - Quick wins with clear metric targets
        - Resource requirements
        - Expected outcomes based on observed patterns

        CRITICAL THINKING REQUIREMENTS:
        - Question whether differences are meaningful
        - Consider alternative interpretations
        - Note where additional data would be valuable
        - Highlight assumptions that need validation

        Keep response concise and business-focused. Every insight must be directly supported by the data shown. When suggesting actions, connect them explicitly to patterns in the data. Always end with "Toggle to 'Discussion Mode' to probe and learn more."
        """
        
        response = self.llm.invoke([{"role": "user", "content": analysis_prompt}])
        
        # Log analysis
        self._log_interaction(
            'analysis',
            original_question=question,
            analysis_response=response.content,
            data_shape={'rows': len(df), 'columns': len(df.columns)}
        )
        
        # Initialize analysis conversation
        self.analysis_memory.clear()
        self.analysis_memory.chat_memory.add_user_message(question)
        self.analysis_memory.chat_memory.add_ai_message(response.content)
        
        return response.content
    
    def continue_analysis(self, follow_up: str, df: pd.DataFrame, original_question: str, config=None) -> str:
        """Continue analysis conversation about the results."""
        business_context = self._get_business_context(config)
        field_context = self._get_field_context(df, config)
        conversation_history = self._format_analysis_history()
        
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
        
        Data Summary:
        - Row Count: {len(df)}
        - Columns: {', '.join(df.columns)}
        - Numeric Summary: {df.describe().to_string() if not df.empty else 'No numeric data'}
        
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
        
        response = self.llm.invoke([{"role": "user", "content": analysis_prompt}])
        
        # Log follow-up analysis
        self._log_interaction(
            'follow_up_analysis',
            original_question=original_question,
            follow_up_question=follow_up,
            analysis_response=response.content,
            data_shape={'rows': len(df), 'columns': len(df.columns)}
        )
        
        # Add to analysis conversation history
        self.analysis_memory.chat_memory.add_user_message(follow_up)
        self.analysis_memory.chat_memory.add_ai_message(response.content)
        
        return response.content

# Initialize query generator
def get_query_generator():
    if 'query_generator' not in st.session_state:
        st.session_state.query_generator = QueryGenerator()
    return st.session_state.query_generator

def generate_dynamic_query(question: str, config=None) -> str:
    return get_query_generator().generate_query(question, config)

def execute_dynamic_query(query: str, question: str = None) -> pd.DataFrame:
    return get_query_generator().execute_query(query)
