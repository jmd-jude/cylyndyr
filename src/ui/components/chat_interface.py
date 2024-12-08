"""Chat interface UI component."""
import streamlit as st
import pandas as pd
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
        
        # Initialize session state
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []
        if 'current_thread' not in st.session_state:
            st.session_state.current_thread = None

    def handle_user_input(self, prompt: str) -> Union[pd.DataFrame, str]:
        """Handle user input and return results."""
        with st.spinner("Generating SQL query..."):
            query = generate_dynamic_query(prompt)
        
        with st.expander("Generated SQL"):
            st.code(query, language='sql')
        
        with st.spinner("Executing query..."):
            result = execute_dynamic_query(query)
            
        # Save to history
        memory_manager.save_interaction(prompt, query, result)
        
        return result

    def render_sidebar(self):
        """Render the sidebar with recent queries and schema configuration."""
        st.subheader("Recent Queries")
        if st.session_state.chat_history:
            for thread_id, thread_info in st.session_state.chat_history:
                if st.button(
                    thread_info.get('question', 'Untitled Query'),
                    key=f"history_{thread_id}",
                    use_container_width=True
                ):
                    st.session_state.current_thread = thread_id
                    st.rerun()
        else:
            st.info("No recent queries")
        
        # Schema configuration in collapsible expander
        with st.expander("Schema Configuration", expanded=False):
            self.schema_editor.render()

    def render_chat(self):
        """Render the chat interface."""
        if not st.session_state.active_connection_id:
            st.info("Please select or add a connection to start querying")
            return
            
        # Display current thread if selected
        if st.session_state.current_thread:
            for thread_id, thread_info in st.session_state.chat_history:
                if thread_id == st.session_state.current_thread:
                    st.chat_message("user").write(thread_info['question'])
                    if 'query' in thread_info:
                        with st.expander("Generated SQL"):
                            st.code(thread_info['query'], language='sql')
                    if 'result' in thread_info:
                        st.chat_message("assistant").write(thread_info['result'])
                    break
        
        # Chat input
        if prompt := st.chat_input("Ask a question about your data..."):
            st.chat_message("user").write(prompt)
            result = self.handle_user_input(prompt)
            
            if isinstance(result, pd.DataFrame):
                st.chat_message("assistant").dataframe(result)
                st.session_state.current_results = result
                st.session_state.current_question = prompt
            else:
                st.chat_message("assistant").write(result)
            
            st.rerun()

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
