"""Chat interface UI component."""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Union

from src.langchain_components.qa_chain import (
    generate_dynamic_query,
    execute_dynamic_query,
    get_query_generator
)

class ChatInterfaceUI:
    """Chat interface UI component."""
    
    def __init__(self, schema_editor):
        """Initialize chat interface UI component."""
        self.schema_editor = schema_editor
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []

    def _handle_sql_generation(self, prompt: str) -> Union[pd.DataFrame, str]:
        """Handle SQL generation mode."""
        with st.spinner("Working on it..."):
            schema_config = self.schema_editor.db_manager.get_schema_config(st.session_state.active_connection_id)
            query = generate_dynamic_query(prompt, config=schema_config.get('config') if schema_config else None)
        
        # Only show SQL to admin users
        if st.session_state.get('is_admin', False):
            with st.expander("Generated SQL"):
                st.code(query, language='sql')
        
        with st.spinner("Almost there..."):
            result = execute_dynamic_query(query)
            
            st.session_state.chat_history.append({
                'timestamp': datetime.now().isoformat(),
                'question': prompt,
                'query': query,
                'result': result
            })
            
            return result

    def _handle_analysis_conversation(self, prompt: str) -> str:
        """Handle analysis conversation mode."""
        with st.spinner("Analyzing..."):
            schema_config = self.schema_editor.db_manager.get_schema_config(st.session_state.active_connection_id)
            response = get_query_generator().continue_analysis(
                prompt,
                st.session_state.current_results,
                st.session_state.current_question,
                config=schema_config.get('config') if schema_config else None
            )
            
            st.session_state.chat_history.append({
                'timestamp': datetime.now().isoformat(),
                'question': prompt,
                'type': 'analysis',
                'result': response
            })
            
            return response

    def handle_user_input(self, prompt: str) -> Union[pd.DataFrame, str]:
        """Handle user input based on current mode."""
        if st.session_state.analysis_mode:
            return self._handle_analysis_conversation(prompt)
        else:
            return self._handle_sql_generation(prompt)

    def render_sidebar(self):
        """Render the sidebar with recent queries."""
        st.subheader("Recent Questions")
        
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🗑️ Clear", help="Clear conversation history"):
                st.session_state.chat_history = []
                st.session_state.current_results = None
                st.session_state.current_question = None
                st.session_state.analysis_mode = False  # Reset mode on clear
                st.rerun()
        
        if st.session_state.chat_history:
            for interaction in reversed(st.session_state.chat_history[-7:]):
                timestamp = datetime.fromisoformat(interaction['timestamp'])
                with st.expander(f"{timestamp.strftime('%I:%M %p')} - {interaction['question'][:30]}..."):
                    st.write("Question:")
                    st.write(interaction['question'])
                    if interaction.get('type') == 'analysis':
                        st.write("Analysis:")
                        st.write(interaction['result'])
                    else:
                        # Only show SQL in history to admin users
                        if st.session_state.get('is_admin', False):
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
        
        # Update input placeholder based on mode
        placeholder = (
            "What patterns or trends are you curious about?"
            if st.session_state.analysis_mode
            else "Ask a question about your data..."
        )
        
        if prompt := st.chat_input(placeholder):
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
        
        # Update subheader based on mode
        subheader = (
            "Explore Results"
            if st.session_state.analysis_mode
            else "Ask Questions, Get Answers"
        )
        st.subheader(subheader)
        
        self.render_chat()
