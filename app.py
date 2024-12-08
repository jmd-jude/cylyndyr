"""Main application entry point."""
from dotenv import load_dotenv
load_dotenv()  # Load environment variables first

import streamlit as st
import uuid
from datetime import datetime
import pandas as pd
from src.ui.components.chat_interface import ChatInterfaceUI
from src.ui.components.schema_editor import SchemaEditorUI
from src.ui.components.login import LoginUI
from src.database.db_manager import DatabaseManager
from src.langchain_components.qa_chain import get_openai_client

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

def generate_result_narrative(df: pd.DataFrame, question: str) -> str:
    """Generate a narrative analysis of the query results."""
    prompt = f"""
    Analyze this query result and provide a brief, business-focused summary.
    Question: {question}
    Data Summary: {df.describe().to_string()}
    Row Count: {len(df)}
    Column Names: {', '.join(df.columns)}
    
    Focus on:
    1. Key insights and patterns
    2. Business implications
    3. Notable trends or anomalies
    
    Keep response clear and concise, under 3-4 sentences.
    """
    
    llm = get_openai_client()
    response = llm.invoke([{"role": "user", "content": prompt}])
    return response.content

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
        
        # Schema editor section
        if st.session_state.get('active_connection_id'):
            st.divider()
            st.subheader("Schema Configuration")
            schema_editor.render()
            
            # Chat history
            st.divider()
            chat_interface.render_history()

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
    db_manager = DatabaseManager()
    chat_interface = ChatInterfaceUI(db_manager)
    schema_editor = SchemaEditorUI(db_manager)
    
    # Render sidebar with connection management and history
    render_sidebar(login_ui, schema_editor, chat_interface)
    
    # Main content area
    st.title("Talk to Your Data")
    
    # Show connection required message if no active connection
    if not st.session_state.get('active_connection_id'):
        st.warning("👈 Please select or add a connection in the sidebar to get started.")
        return
    
    # Main app layout when connection is active
    st.subheader("Ask Questions, Get Answers")
    
    # Display chat interface
    user_input = st.chat_input("Ask a question about your data...")
    if user_input:
        results = chat_interface.handle_user_input(user_input)
        if isinstance(results, pd.DataFrame):
            st.session_state.current_results = results
            st.session_state.current_question = user_input
    
    # Add analyze button for current results
    if st.session_state.current_results is not None:
        if st.button("📊 Analyze This Result", key="analyze_button"):
            with st.spinner("Analyzing..."):
                narrative = generate_result_narrative(
                    st.session_state.current_results,
                    st.session_state.current_question
                )
                st.info(narrative)

if __name__ == "__main__":
    main()
