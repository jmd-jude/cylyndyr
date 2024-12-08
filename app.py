"""Main application entry point."""
import os
from dotenv import load_dotenv
load_dotenv()  # Load environment variables first

import streamlit as st
import uuid
from datetime import datetime
import pandas as pd
from src.ui.components.chat_interface import ChatInterfaceUI
from src.ui.components.schema_editor import SchemaEditorUI
from src.ui.components.login import LoginUI
from src.database.db_manager import get_database_manager
from src.langchain_components.qa_chain import query_generator

# Debug: Print environment at startup
print("Environment variables at startup:")
print(f"DATABASE_URL={os.getenv('DATABASE_URL')}")
print(f"Current working directory: {os.getcwd()}")

# Configure Streamlit page
st.set_page_config(
    page_title="Cylyndyr",
    page_icon="🔍",
    layout="wide"
)

def initialize_session_state():
    """Initialize all required session state variables."""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if 'user_id' not in st.session_state:
        st.session_state.user_id = None
    if 'username' not in st.session_state:
        st.session_state.username = None
    if 'show_success' not in st.session_state:
        st.session_state.show_success = False
    if 'active_connection_id' not in st.session_state:
        st.session_state.active_connection_id = None
    if 'active_connection_name' not in st.session_state:
        st.session_state.active_connection_name = None
    if 'current_results' not in st.session_state:
        st.session_state.current_results = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None

def render_sidebar(login_ui: LoginUI, schema_editor: SchemaEditorUI, chat_interface: ChatInterfaceUI):
    """Render sidebar with user info and connection management."""
    with st.sidebar:
        # User section
        st.write(f"👤 Logged in as: {st.session_state.username}")
        if st.button("🚪 Logout", type="secondary"):
            login_ui.logout()
            
        st.divider()
        
        # Connection management section
        st.subheader("Connection Management")
        
        # Active connection info
        if st.session_state.get('active_connection_id'):
            st.info(f"Active: {st.session_state.get('active_connection_name', 'Unknown')}")
        
        # Add new connection dropdown
        schema_editor.render_add_connection()
        
        st.divider()
        
        # Existing connections dropdown
        st.subheader("Existing Connections")
        schema_editor.render_connection_selector()
        
        # Recent queries and schema editor
        if st.session_state.get('active_connection_id'):
            st.divider()
            chat_interface.render_sidebar()

def main():
    """Main application entry point."""
    # Initialize session state
    initialize_session_state()
    
    # Initialize components
    login_ui = LoginUI()
    
    # Check if user is logged in
    if not login_ui.is_logged_in():
        login_ui.render_login()
        return
    
    # Initialize other components only after login
    db_manager = get_database_manager()
    schema_editor = SchemaEditorUI(db_manager)
    chat_interface = ChatInterfaceUI(schema_editor)
    
    # Render sidebar with connection management and history
    render_sidebar(login_ui, schema_editor, chat_interface)
    
    # Main content area
    chat_interface.render()
    
    # Add analyze button for current results
    if st.session_state.current_results is not None:
        if st.button("📊 Analyze This Result", key="analyze_button"):
            with st.spinner("Analyzing..."):
                schema_config = schema_editor.db_manager.get_schema_config(st.session_state.active_connection_id)
                narrative = query_generator.analyze_result(
                    st.session_state.current_results,
                    st.session_state.current_question,
                    config=schema_config.get('config') if schema_config else None
                )
                st.info(narrative)

if __name__ == "__main__":
    main()
