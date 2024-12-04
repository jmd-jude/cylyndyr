import streamlit as st
# Must be the first Streamlit command
st.set_page_config(initial_sidebar_state="collapsed")

import os
from dotenv import load_dotenv
import uuid

# Add the project root to Python path
from pathlib import Path
import sys
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Import managers
from src.schema_manager import SchemaManager
from src.database.connection_manager import ConnectionManager
from src.database.db_manager import DatabaseManager

# Import UI components
from src.ui.components.connection_manager import ConnectionManagerUI
from src.ui.components.chat_interface import ChatInterfaceUI
from src.ui.components.schema_editor import SchemaEditorUI

# Load environment variables - only in local development
if os.path.exists(".env"):
    load_dotenv(override=True)

# Feature flags
SHOW_SCHEMA_EDITOR = True
MULTI_CONNECTION_ENABLED = True

# Initialize managers
schema_manager = SchemaManager()
connection_manager = ConnectionManager()
db_manager = DatabaseManager()

# Initialize UI components
connection_manager_ui = ConnectionManagerUI(connection_manager, db_manager)
chat_interface = ChatInterfaceUI(db_manager)
schema_editor = SchemaEditorUI(schema_manager)

def initialize_session_state():
    """Initialize or update session state variables."""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    if 'user_id' not in st.session_state:
        # For now, create or get the default user
        try:
            default_user = db_manager.get_user_by_email("default@cylyndyr.com")
            if not default_user:
                user_id = db_manager.create_user(
                    email="default@cylyndyr.com",
                    name="Default User"
                )
            else:
                user_id = default_user['id']
            st.session_state.user_id = user_id
        except Exception as e:
            st.error(f"Error initializing user: {str(e)}")
            st.stop()
    
    if 'active_connection_id' not in st.session_state:
        # Try to get or create default connection
        try:
            connections = db_manager.get_user_connections(st.session_state.user_id)
            if not connections:
                # Migrate existing connection
                connection_id = connection_manager.migrate_existing_connection()
                if not connection_id:
                    st.error("Failed to create default connection")
                    st.stop()
                st.session_state.active_connection_id = connection_id
            else:
                st.session_state.active_connection_id = connections[0]['id']
        except Exception as e:
            st.error(f"Error initializing connection: {str(e)}")
            st.stop()

def check_api_key():
    """Check if OpenAI API key is configured."""
    try:
        api_key = st.secrets.openai.api_key
    except Exception:
        api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        st.error("OpenAI API key not found. Please set the OPENAI_API_KEY in environment variables or Streamlit secrets.")
        st.stop()

def check_active_connection():
    """Check if active connection is valid."""
    if not st.session_state.active_connection_id:
        st.error("No active connection found.")
        st.stop()
    
    if not connection_manager.test_connection(st.session_state.active_connection_id):
        st.error("Could not connect to database. Please check your connection settings.")
        st.stop()

def load_schema_config(has_key):
    """Load schema config if Cylyndyr Key is enabled."""
    if has_key == "Yes":
        try:
            schema_config = db_manager.get_schema_config(st.session_state.active_connection_id)
            return schema_config['config'] if schema_config else None
        except Exception as e:
            st.error(f"Error loading schema config: {str(e)}")
            st.stop()
    return None

def main():
    # Initialize session state
    initialize_session_state()
    
    st.title("Talk to Your Data")
    
    # Check OpenAI API key
    check_api_key()
    
    # Check active connection
    check_active_connection()
    
    # Sidebar Configuration
    with st.sidebar:
        has_key = st.radio(
            "Cylyndyr Key",
            ["Yes", "No"],
            help="Experience the difference with Cylyndyr"
        )
        
        st.markdown("---")
        
        # Connection management
        if MULTI_CONNECTION_ENABLED:
            connection_manager_ui.render_connection_selector()
            connection_manager_ui.render_manager()
            st.markdown("---")
        
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
               """)
        
        st.markdown("---")
        
        # Chat history
        with st.expander("Recent Questions", expanded=False):
            chat_interface.display_chat_history()
    
    # Load schema configuration based on Cylyndyr Key
    config = load_schema_config(has_key)
    
    # Schema editor in sidebar (only if enabled)
    if SHOW_SCHEMA_EDITOR and config:
        schema_editor.render_editor(config, st.session_state.active_connection_id)
    
    # Main query interface
    st.header("Ask Questions, Get Answers")
    
    # Query input using chat_input
    if question := st.chat_input("Ask a question about your data..."):
        chat_interface.handle_user_input(question)
    
    # Handle analysis of previous results when button is clicked
    chat_interface.handle_analysis_request()

if __name__ == "__main__":
    main()
