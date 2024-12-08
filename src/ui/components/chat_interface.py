"""Chat interface UI components."""
import streamlit as st
from datetime import datetime
import pandas as pd
from typing import Optional, Dict, Any, Union

from src.langchain_components.qa_chain import (
    generate_dynamic_query,
    execute_dynamic_query,
    memory_manager
)
from src.database.db_manager import DatabaseManager

class ChatInterfaceUI:
    def __init__(self, db_manager: DatabaseManager):
        """Initialize chat interface UI with database manager."""
        self.db_manager = DatabaseManager()

    def handle_user_input(self, question: str) -> Union[pd.DataFrame, str]:
        """Handle user input and generate response.
        
        Returns:
            DataFrame if query successful, error message string if failed
        """
        try:
            with st.spinner("Working on it..."):
                # Get schema config for active connection
                config = self.get_active_schema_config()
                if not config:
                    st.error("No schema configuration found for the active connection.")
                    return "No schema configuration found"
                
                # Generate SQL query
                sql_query = generate_dynamic_query(question, st.session_state.session_id, config)
                
                # Show the generated SQL with the question context
                with st.expander("Cylyndyr", expanded=False):
                    st.markdown(f"**Question:** {question}")
                    st.markdown("**Cyl:**")
                    st.code(sql_query, language="sql")
                
                # Execute query and show results
                results = execute_dynamic_query(sql_query, question, st.session_state.session_id)
                
                if isinstance(results, pd.DataFrame):
                    # Display results
                    st.dataframe(self.format_dataframe(results))
                    return results
                else:
                    st.error(f"Error executing query: {results}")
                    return f"Error executing query: {results}"
        
        except Exception as e:
            st.error("An error occurred while processing your question.")
            st.error(f"Error details: {str(e)}")
            return f"Error: {str(e)}"

    def get_active_schema_config(self) -> Optional[Dict]:
        """Get schema config for active connection."""
        try:
            schema_config = self.db_manager.get_schema_config(st.session_state.active_connection_id)
            if schema_config:
                return schema_config['config']
            return None
        except Exception as e:
            st.error(f"Error loading schema config: {str(e)}")
            return None

    def format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply formatting to DataFrame."""
        formatted_df = df.copy()
        
        for col in formatted_df.columns:
            if formatted_df[col].empty:
                continue
            
            sample_val = formatted_df[col].dropna().iloc[0] if not formatted_df[col].dropna().empty else None
            if sample_val is None:
                continue
            
            col_lower = col.lower()
            
            # Date formatting
            if isinstance(sample_val, (datetime, pd.Timestamp)) or 'date' in col_lower:
                formatted_df[col] = pd.to_datetime(formatted_df[col]).dt.strftime('%Y-%m-%d')
            
            # Numeric formatting
            elif isinstance(sample_val, (int, float)):
                if formatted_df[col].between(1970, 2030).all():
                    formatted_df[col] = formatted_df[col].astype(int).astype(str)
                elif any(term in col_lower for term in ['sales', 'revenue', 'price', 'amount', 'cost', 'total']):
                    formatted_df[col] = formatted_df[col].round(4).apply(lambda x: f"{x:,.4f}")
                elif formatted_df[col].abs().max() >= 1000:
                    formatted_df[col] = formatted_df[col].round(4).apply(lambda x: f"{x:,.4f}")
        
        return formatted_df

    def render_history(self):
        """Render chat history in sidebar."""
        with st.expander("Recent Questions", expanded=True):
            history = memory_manager.get_chat_history(st.session_state.session_id)
            if not history:
                st.markdown("*No questions asked yet*")
                return

            for interaction in reversed(history):  # Show most recent first
                st.text(datetime.fromisoformat(interaction['timestamp']).strftime("%I:%M %p"))
                st.markdown(f"**Q:** {interaction['question']}")
                
                if st.button("🔍 Show SQL", key=f"sql_{interaction['timestamp']}"):
                    st.code(interaction['query'], language="sql")
                
                st.code(interaction['result'])
                st.divider()
