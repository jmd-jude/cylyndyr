import streamlit as st
# Must be the first Streamlit command
st.set_page_config(initial_sidebar_state="collapsed")

import os
from dotenv import load_dotenv
import yaml
import pandas as pd
import importlib.util
import traceback
import sys
from pathlib import Path
import glob
from datetime import datetime
import uuid
import re
import numpy as np

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Now import our local modules
from src.schema_manager import SchemaManager
from src.database.schema_inspector import inspect_database
from src.langchain_components.qa_chain import generate_dynamic_query, execute_dynamic_query, memory_manager, get_openai_client

# Load environment variables - only in local development
if os.path.exists(".env"):
    load_dotenv(override=True)

# Feature flags
SHOW_SCHEMA_EDITOR = True  # Set to False to hide

# Initialize schema manager
schema_manager = SchemaManager()

def format_dataframe(df):
    """Apply formatting to DataFrame based on column patterns."""
    # Create a copy to avoid modifying the original
    formatted_df = df.copy()
    
    for col in formatted_df.columns:
        # Skip if column is empty
        if formatted_df[col].empty:
            continue
        
        # Get the first non-null value to check type
        sample_val = formatted_df[col].dropna().iloc[0] if not formatted_df[col].dropna().empty else None
        if sample_val is None:
            continue
        
        col_lower = col.lower()
        
        # Date formatting (remove time component)
        if isinstance(sample_val, (datetime, pd.Timestamp)) or 'date' in col_lower:
            formatted_df[col] = pd.to_datetime(formatted_df[col]).dt.strftime('%Y-%m-%d')
        
        # Numeric formatting
        elif isinstance(sample_val, (int, float, np.number)):
            # Check if column contains years
            if (formatted_df[col].between(1970, 2030).all() and 
                formatted_df[col].astype(int).astype(float).eq(formatted_df[col]).all()):
                # Year values - keep as is
                formatted_df[col] = formatted_df[col].astype(int).astype(str)
            
            # Sales/Currency formatting
            elif any(term in col_lower for term in ['sales', 'revenue', 'price', 'amount', 'cost', 'total']):
                formatted_df[col] = formatted_df[col].round(0).astype(int).apply(lambda x: f"{x:,}")
            
            # Large number formatting
            elif formatted_df[col].abs().max() >= 1000:
                formatted_df[col] = formatted_df[col].round().astype(np.int64).apply(lambda x: f"{x:,}")
    
    return formatted_df

def get_snowflake_credentials():
    """Get Snowflake credentials from environment or streamlit secrets."""
    try:
        # Try to get from streamlit secrets first (for cloud deployment)
        account = st.secrets.snowflake.account
        # Remove any duplicate .snowflakecomputing.com
        if '.snowflakecomputing.com' in account:
            account = account.replace('.snowflakecomputing.com', '')
        
        return {
            'account': account,
            'user': st.secrets.snowflake.user,
            'password': st.secrets.snowflake.password,
            'database': st.secrets.snowflake.database,
            'warehouse': st.secrets.snowflake.warehouse,
            'schema': st.secrets.snowflake.schema
        }
    except Exception:
        # Fall back to environment variables (for local development)
        account = os.getenv('SNOWFLAKE_ACCOUNT', '')
        if '.snowflakecomputing.com' in account:
            account = account.replace('.snowflakecomputing.com', '')
            
        return {
            'account': account,
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }

def check_api_key():
    """Check if OpenAI API key is configured."""
    try:
        api_key = st.secrets.openai.api_key
    except Exception:
        api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        st.error("OpenAI API key not found. Please set the OPENAI_API_KEY in environment variables or Streamlit secrets.")
        st.stop()

def check_snowflake_config():
    """Check Snowflake configuration."""
    creds = get_snowflake_credentials()
    missing_vars = [key for key, value in creds.items() if not value]
    
    if missing_vars:
        st.error(f"Missing Snowflake configuration: {', '.join(missing_vars)}")
        st.info("Please set these values in Streamlit secrets or environment variables.")
        st.stop()

def load_schema_config(has_key):
    """Load schema config if Cylyndyr Key is enabled."""
    if has_key == "Yes":
        try:
            return schema_manager.load_config("snowflake")
        except Exception as e:
            st.error(f"Error loading schema config: {str(e)}")
            st.stop()
    return None

def schema_editor(config):
    """Simple schema configuration editor."""
    with st.sidebar.expander("Configuration Manager"):
        # Business Context
        st.subheader("Business Context")
        description = st.text_area(
            "Business Description",
            value=config.get('business_context', {}).get('description', ''),
            key="business_desc"
        )
        
        # Key Concepts
        concepts = config.get('business_context', {}).get('key_concepts', [])
        concepts_text = st.text_area(
            "Key Business Concepts (one per line)",
            value='\n'.join(concepts) if concepts else '',
            key="key_concepts"
        )
        
        if st.button("Update Business Context"):
            schema_manager.update_business_context(
                "snowflake",
                description,
                concepts_text.split('\n') if concepts_text else []
            )
            st.success("Business context updated!")
        
        # Table Descriptions
        st.subheader("Table Descriptions")
        selected_table = st.selectbox(
            "Select Table",
            schema_manager.get_tables("snowflake")
        )
        
        if selected_table:
            table_desc = st.text_area(
                f"Description for {selected_table}",
                value=config['tables'][selected_table].get('description', ''),
                key=f"table_desc_{selected_table}"
            )
            
            if st.button(f"Update {selected_table} Description"):
                schema_manager.update_table_description("snowflake", selected_table, table_desc)
                st.success(f"Updated description for {selected_table}!")
            
            # Field Descriptions
            st.subheader("Field Descriptions")
            selected_field = st.selectbox(
                "Select Field",
                schema_manager.get_fields("snowflake", selected_table)
            )
            
            if selected_field:
                field_desc = st.text_area(
                    f"Description for {selected_field}",
                    value=config['tables'][selected_table]['fields'][selected_field].get('description', ''),
                    key=f"field_desc_{selected_table}_{selected_field}"
                )
                
                if st.button(f"Update {selected_field} Description"):
                    schema_manager.update_field_description(
                        "snowflake", selected_table, selected_field, field_desc
                    )
                    st.success(f"Updated description for {selected_field}!")

def format_result(result):
    """Format the result for display in chat history."""
    if isinstance(result, pd.DataFrame):
        return f"DataFrame with {len(result)} rows and {len(result.columns)} columns"
    # For pre-formatted strings (from QueryMemoryManager), display as is
    return result

def display_chat_history():
    """Display chat history in a clean format."""
    # Get user-specific session ID
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    history = memory_manager.get_chat_history(st.session_state.session_id)
    if not history:
        st.markdown("*No questions asked yet*")
        return

    for i, interaction in enumerate(reversed(history)):  # Show most recent first
        # Format timestamp
        timestamp = datetime.fromisoformat(interaction['timestamp'])
        time_str = timestamp.strftime("%I:%M %p")  # e.g., "2:30 PM"
        
        # Create columns for timestamp and content
        cols = st.columns([1, 4])
        with cols[0]:
            st.text(time_str)
        with cols[1]:
            # Display question
            st.markdown(f"**Q:** {interaction['question']}")
            
            # Display SQL query with a toggle button
            if st.button(f"🔍 Toggle Cyl", key=f"sql_toggle_{i}"):
                st.code(interaction['query'], language="sql")
            
            # Display result
            st.markdown("**Result:**")
            # Use st.code for pre-formatted results to preserve spacing
            st.code(interaction['result'])
        
        # Add a subtle divider between interactions
        if i < len(history) - 1:
            st.divider()

def generate_result_narrative(df, question):
    """Generate a narrative analysis of the query results."""
    prompt = f"""
    Analyze this query result and provide a brief, business-focused summary.
    Question: {question}
    Data Summary: {df.describe().to_string()}
    Row Count: {len(df)}
    
    Focus on:
    1. Key insights
    2. Notable patterns
    3. Business implications
    
    Keep response under 3 sentences.
    """
    llm = get_openai_client()
    response = llm.invoke([{"role": "user", "content": prompt}])
    return response.content

def main():
    # Initialize session state for user ID if not exists
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    # Initialize session state for current results and question
    if 'current_results' not in st.session_state:
        st.session_state.current_results = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None
    
    st.title("Talk to Your Data")
    
    # Check OpenAI API key with detailed feedback
    check_api_key()
    
    # Check Snowflake configuration
    check_snowflake_config()
    
    # Sidebar Configuration
    with st.sidebar:
        has_key = st.radio(
            "Cylyndyr Key",
            ["Yes", "No"],
            help="Experience the difference with Cylyndyr"
        )
        
        st.markdown("---")  # Visual separator
        
        # INFO SECTION
        with st.expander("Notes", expanded=False):
            st.markdown("""
               **Simple Questions:**
                - How many customers do we have?
                - What's the total value of all orders?

                **Intermediate Questions:**
                - What's the average order value by region?
                - Show order trends over time by region

                **Complex Questions:**
                - What's the average delivery time by product category?
                - Show me customer order patterns across different regions
                - Calculate market share by supplier within each region
                        
                **Database**
                - CUSTOMERS
                - LINEITEM
                - NATION
                - ORDERS
                - PART
                - PARTSUPP
                - REGION
                - SUPPLIER
               """)
        
        st.markdown("---")  # Visual separator
        
        # Chat history moved below example questions and in expander
        with st.expander("Recent Questions", expanded=False):
            display_chat_history()
    
    # Load schema configuration based on Cylyndyr Key
    config = load_schema_config(has_key)
    
    # Schema editor in sidebar (only if enabled)
    if SHOW_SCHEMA_EDITOR and config:
        schema_editor(config)
    
    # Main query interface
    st.header("Ask Questions, Get Answers")
    
    # Query input using chat_input
    if question := st.chat_input("Ask a question about your data..."):
        try:
            with st.spinner("Working on it..."):
                # Generate SQL query using user's session ID
                sql_query = generate_dynamic_query(question, st.session_state.session_id, config)
                
                # Show the generated SQL with the question context
                with st.expander("Cylyndyr", expanded=False):
                    st.markdown(f"**Question:** {question}")
                    st.markdown("**Cyl:**")
                    st.code(sql_query, language="sql")
                
                # Execute query and show results
                results = execute_dynamic_query(sql_query, question, st.session_state.session_id)
                
                if isinstance(results, pd.DataFrame):
                    # Store current results and question in session state
                    st.session_state.current_results = results
                    st.session_state.current_question = question
                    
                    # Apply formatting before display
                    formatted_results = format_dataframe(results)
                    st.dataframe(formatted_results)
                    
                    # Add analyze button for non-empty results
                    if not formatted_results.empty:
                        if st.button("📊 Analyze This Result", key="analyze_button"):
                            with st.spinner("Analyzing..."):
                                narrative = generate_result_narrative(formatted_results, question)
                                st.info(narrative)
                else:
                    st.error(f"Error executing query: {results}")
        
        except Exception as e:
            st.error("An error occurred while processing your question.")
            st.error(f"Error details: {str(e)}")
    
    # Handle analysis of previous results when button is clicked
    elif st.session_state.current_results is not None and st.button("📊 Analyze This Result", key="analyze_button"):
        with st.spinner("Analyzing..."):
            narrative = generate_result_narrative(
                st.session_state.current_results,
                st.session_state.current_question
            )
            st.info(narrative)

if __name__ == "__main__":
    main()
