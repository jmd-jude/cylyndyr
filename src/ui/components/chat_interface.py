"""Chat interface UI component."""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, Union

from src.langchain_components.qa_chain import (
    generate_dynamic_query,
    execute_dynamic_query,
    memory_manager
)

class ChatInterfaceUI:
    """Chat interface UI component."""
    
    def __init__(self, schema_editor):
        """Initialize chat interface UI component."""
        self.schema_editor = schema_editor

    def handle_user_input(self, prompt: str) -> Union[pd.DataFrame, str]:
        """Handle user input and return results."""
        with st.spinner("Generating SQL query..."):
            schema_config = self.schema_editor.db_manager.get_schema_config(st.session_state.active_connection_id)
            query = generate_dynamic_query(prompt, config=schema_config.get('config') if schema_config else None)
        
        with st.expander("Generated SQL"):
            st.code(query, language='sql')
        
        with st.spinner("Executing query..."):
            result = execute_dynamic_query(query, prompt)
            
        return result

    def render_sidebar(self):
        """Render the sidebar with recent queries."""
        st.subheader("Recent Questions")
        
        history = memory_manager.get_chat_history()
        if history:
            for interaction in history:
                timestamp = datetime.fromisoformat(interaction['timestamp'])
                with st.expander(f"{timestamp.strftime('%I:%M %p')}"):
                    st.write(f"Q: {interaction['question']}")
                    st.write("Result:")
                    st.code(interaction['result'])
                    st.button("🔄 Toggle Cyl", key=f"toggle_{interaction['timestamp']}")
        else:
            st.info("No recent questions")
            
        # Schema editor
        with st.expander("Schema Configuration"):
            self.schema_editor.render()

    def render_chat(self):
        """Render the chat interface."""
        if not st.session_state.active_connection_id:
            st.info("Please select or add a connection to start querying")
            return
        
        # Chat input
        if prompt := st.chat_input("Ask a question about your data..."):
            st.chat_message("user").write(prompt)
            result = self.handle_user_input(prompt)
            
            if isinstance(result, pd.DataFrame):
                st.chat_message("assistant").dataframe(result)
            else:
                st.chat_message("assistant").write(result)

    def render(self):
        """Render the main chat interface."""
        st.title("Talk to Your Data")
        
        # Show connection required message if no active connection
        if not st.session_state.get('active_connection_id'):
            st.warning("👈 Please select or add a connection in the sidebar to get started.")
            return
        
        # Main chat interface
        st.subheader("Ask Questions, Get Answers")
        self.render_chat()
