"""Chat interface UI component."""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Union

from src.query.generator import (
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
        # Set current question in session state BEFORE generating query
        st.session_state.current_question = prompt
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
            
            # Limit chat history to last 10 items
            if len(st.session_state.chat_history) > 10:
                st.session_state.chat_history = st.session_state.chat_history[-10:]
            
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
            
            # Limit chat history to last 10 items
            if len(st.session_state.chat_history) > 10:
                st.session_state.chat_history = st.session_state.chat_history[-10:]
            
            return response

    def handle_user_input(self, prompt: str) -> Union[pd.DataFrame, str]:
        """Handle user input based on current mode."""
        if st.session_state.analysis_mode:
            return self._handle_analysis_conversation(prompt)
        else:
            return self._handle_sql_generation(prompt)

    def render_sidebar(self):
        """Render the sidebar with recent queries from database."""
        st.subheader("Recent Questions")
        
        # Get recent queries from database instead of session state
        if st.session_state.get('user_id') and st.session_state.get('active_connection_id'):
            recent_queries = self.schema_editor.db_manager.get_user_query_history(
                user_id=st.session_state.user_id,
                connection_id=st.session_state.active_connection_id,
                limit=5  # Limit to N most recent
            )
            
            if recent_queries:
                # Create a container with max height for scrolling
                with st.container():
                    for query in recent_queries[:7]:  # Show only 7 in UI to avoid clutter
                        # Parse created_at and format nicely
                        from datetime import datetime
                        try:
                            created_at = datetime.fromisoformat(query['created_at'].replace('Z', '+00:00'))
                            time_str = created_at.strftime('%I:%M %p')
                        except:
                            time_str = "Recent"
                        
                        with st.expander(f"{time_str} - {query['question'][:30]}...", expanded=False):
                            st.write("**Question:**")
                            st.write(query['question'])
                            
                            # Only show SQL in history to admin users
                            if st.session_state.get('is_admin', False):
                                st.write("**SQL Query:**")
                                st.code(query['generated_sql'], language='sql')
                            
                            st.write("**Result:**")
                            if query['result_preview']:
                                import json
                                try:
                                    preview_data = json.loads(query['result_preview']) if isinstance(query['result_preview'], str) else query['result_preview']
                                    if preview_data:
                                        import pandas as pd
                                        df = pd.DataFrame(preview_data)
                                        st.dataframe(df, height=200)  # Limit height
                                except:
                                    st.write("Preview unavailable")
                            
                            # Compact favorite button
                            if st.button(f"⭐", key=f"fav_{query['id']}", help="Add to favorites"):
                                if self.schema_editor.db_manager.toggle_query_favorite(query['id'], st.session_state.user_id):
                                    st.success("⭐ Favorited!")
                                    st.rerun()
                    
                    if len(recent_queries) > 7:
                        st.caption(f"Showing 7 of {len(recent_queries)} recent queries")
            else:
                st.info("No recent questions for this connection")
        else:
            st.info("Select a connection to see query history")
            
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
