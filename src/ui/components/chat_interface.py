"""Chat interface UI component."""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Union

from src.langchain_components.qa_chain import (
    generate_dynamic_query,
    execute_dynamic_query,
    query_generator
)

class ChatInterfaceUI:
    """Chat interface UI component."""
    
    def __init__(self, schema_editor):
        """Initialize chat interface UI component."""
        self.schema_editor = schema_editor
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []

    def handle_user_input(self, prompt: str) -> Union[pd.DataFrame, str]:
        """Handle user input and return results."""
        with st.spinner("Generating SQL query..."):
            schema_config = self.schema_editor.db_manager.get_schema_config(st.session_state.active_connection_id)
            query = generate_dynamic_query(prompt, config=schema_config.get('config') if schema_config else None)
        
        with st.expander("Generated SQL"):
            st.code(query, language='sql')
        
        with st.spinner("Executing query..."):
            result = execute_dynamic_query(query)
            
            st.session_state.chat_history.append({
                'timestamp': datetime.now().isoformat(),
                'question': prompt,
                'query': query,
                'result': result
            })
            
            return result

    def render_sidebar(self):
        """Render the sidebar with recent queries."""
        st.subheader("Recent Questions")
        
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🗑️ Clear", help="Clear conversation history"):
                st.session_state.chat_history = []
                st.session_state.current_results = None
                st.session_state.current_question = None
                st.rerun()
        
        if st.session_state.chat_history:
            for interaction in reversed(st.session_state.chat_history[-7:]):
                timestamp = datetime.fromisoformat(interaction['timestamp'])
                with st.expander(f"{timestamp.strftime('%I:%M %p')} - {interaction['question'][:30]}..."):
                    st.write("Question:")
                    st.write(interaction['question'])
                    st.write("SQL Query:")
                    st.code(interaction['query'], language='sql')
                    st.write("Result:")
                    if isinstance(interaction['result'], pd.DataFrame):
                        st.dataframe(interaction['result'])
                    else:
                        st.write(interaction['result'])
        else:
            st.info("No recent questions")
            
        with st.expander("Schema Configuration"):
            self.schema_editor.render()

    def render_chat(self):
        """Render the chat interface."""
        if not st.session_state.active_connection_id:
            st.info("Please select or add a connection to start querying")
            return
        
        if prompt := st.chat_input("Ask a question about your data..."):
            st.chat_message("user").write(prompt)
            result = self.handle_user_input(prompt)
            
            msg = st.chat_message("assistant")
            if isinstance(result, pd.DataFrame):
                # Store current results for analysis
                st.session_state.current_results = result
                st.session_state.current_question = prompt
                
                msg.dataframe(result, use_container_width=True, hide_index=True)
            else:
                msg.write(result)

    def render(self):
        """Render the main chat interface."""
        st.title("Talk to Your Data")
        
        if not st.session_state.get('active_connection_id'):
            st.warning("👈 Please select or add a connection in the sidebar to get started.")
            return
        
        st.subheader("Ask Questions, Get Answers")
        self.render_chat()
