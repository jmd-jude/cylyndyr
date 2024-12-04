"""Chat interface UI components."""
import streamlit as st
from datetime import datetime
import pandas as pd
from typing import Optional, Dict, Any

from src.langchain_components.qa_chain import (
    generate_dynamic_query,
    execute_dynamic_query,
    memory_manager,
    get_openai_client
)
from src.database.db_manager import DatabaseManager

class ChatInterfaceUI:
    def __init__(self, db_manager: DatabaseManager):
        """Initialize chat interface UI with database manager."""
        self.db_manager = db_manager
        if 'current_results' not in st.session_state:
            st.session_state.current_results = None
        if 'current_question' not in st.session_state:
            st.session_state.current_question = None

    def get_active_schema_config(self) -> Optional[Dict]:
        """Get schema config for active connection."""
        try:
            schema_config = self.db_manager.get_schema_config(st.session_state.active_connection_id)
            if schema_config:
                # Get connection details
                conn = self.db_manager.get_connection(st.session_state.active_connection_id)
                st.write(f"Using schema config for connection: {conn['name']}")
                st.write(f"Database: {conn['config']['database']}")
                st.write(f"Schema: {conn['config']['schema']}")
                
                # Show available tables
                if schema_config['config'].get('tables'):
                    st.write("Available tables:", list(schema_config['config']['tables'].keys()))
                else:
                    st.write("No tables found in schema config")
                
                return schema_config['config']
        except Exception as e:
            st.error(f"Error loading schema config: {str(e)}")
        return None

    def format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
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
            elif isinstance(sample_val, (int, float)):
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
                    formatted_df[col] = formatted_df[col].round().astype(int).apply(lambda x: f"{x:,}")
        
        return formatted_df

    def generate_result_narrative(self, df: pd.DataFrame, question: str) -> str:
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

    def display_chat_history(self):
        """Display chat history in a clean format."""
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
                if st.button(f"🔍 Toggle Cyl", key=f"sql_toggle_{i}_{interaction['timestamp']}"):
                    st.code(interaction['query'], language="sql")
                
                # Display result
                st.markdown("**Result:**")
                # Use st.code for pre-formatted results to preserve spacing
                st.code(interaction['result'])
            
            # Add a subtle divider between interactions
            if i < len(history) - 1:
                st.divider()

    def handle_user_input(self, question: str):
        """Handle user input and generate response."""
        try:
            with st.spinner("Working on it..."):
                # Get schema config for active connection
                config = self.get_active_schema_config()
                if not config:
                    st.error("No schema configuration found for the active connection.")
                    return
                
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
                    formatted_results = self.format_dataframe(results)
                    st.dataframe(formatted_results)
                    
                    # Add analyze button for non-empty results
                    if not formatted_results.empty:
                        if st.button("📊 Analyze This Result", key=f"analyze_button_{datetime.now().timestamp()}"):
                            with st.spinner("Analyzing..."):
                                narrative = self.generate_result_narrative(formatted_results, question)
                                st.info(narrative)
                else:
                    st.error(f"Error executing query: {results}")
        
        except Exception as e:
            st.error("An error occurred while processing your question.")
            st.error(f"Error details: {str(e)}")

    def handle_analysis_request(self):
        """Handle request to analyze previous results."""
        if st.session_state.current_results is not None:
            timestamp = datetime.now().timestamp()
            if st.button("📊 Analyze Previous Result", key=f"analyze_prev_{timestamp}"):
                with st.spinner("Analyzing..."):
                    narrative = self.generate_result_narrative(
                        st.session_state.current_results,
                        st.session_state.current_question
                    )
                    st.info(narrative)
