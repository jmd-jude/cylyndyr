"""Connection management UI components."""
import streamlit as st
from typing import Optional

from src.database.connection_manager import ConnectionManager
from src.database.db_manager import DatabaseManager
from src.database.schema_inspector import inspect_database
from src.ui.components.connection_form import ConnectionForm
from src.ui.components.connection_list import ConnectionList
from src.ui.components.connection_selector import ConnectionSelector

class ConnectionManagerUI:
    def __init__(self, connection_manager: ConnectionManager, db_manager: DatabaseManager):
        """Initialize connection manager UI with required managers."""
        self.connection_manager = connection_manager
        self.db_manager = db_manager

    def handle_test_connection(self, connection_id: str):
        """Handle test connection button click."""
        if self.connection_manager.test_connection(connection_id):
            st.success("Connection test successful!")
        else:
            st.error("Connection test failed")

    def handle_connection_change(self, connection_id: str):
        """Handle connection selection change."""
        if connection_id != st.session_state.active_connection_id:
            st.session_state.active_connection_id = connection_id
            conn = self.db_manager.get_connection(connection_id)
            st.success(f"Switched to connection: {conn['name']} ({conn['config']['database']}.{conn['config']['schema']})")
            st.rerun()

    def render_manager(self):
        """Render the complete connection manager UI."""
        with st.sidebar:
            # Current connection status
            current_conn = self.db_manager.get_connection(st.session_state.active_connection_id)
            st.info(f"Active Connection: {current_conn['name']} ({current_conn['config']['database']}.{current_conn['config']['schema']})")
            
            # Add new connection section
            with st.expander("Add New Connection", expanded=False):
                st.markdown("### Add New Connection")
                form_data = ConnectionForm.render()
                
                if form_data:
                    connection_id = self.connection_manager.create_snowflake_connection(
                        user_id=st.session_state.user_id,
                        name=form_data['name'],
                        credentials=form_data['credentials']
                    )
                    
                    if connection_id:
                        st.success("Connection added successfully!")
                        
                        # Test the connection
                        if self.connection_manager.test_connection(connection_id):
                            st.success("Connection test successful!")
                            # Get connection details
                            connection = self.db_manager.get_connection(connection_id)
                            if connection:
                                try:
                                    # Inspect database and create schema config
                                    config = inspect_database(
                                        db_type="snowflake",
                                        **connection['config']
                                    )
                                    self.db_manager.create_schema_config(
                                        connection_id=connection_id,
                                        user_id=st.session_state.user_id,
                                        config=config
                                    )
                                    st.success("Schema configuration created!")
                                    
                                    # Switch to the new connection
                                    self.handle_connection_change(connection_id)
                                except Exception as e:
                                    st.error(f"Error creating schema config: {str(e)}")
                        else:
                            st.error("Connection test failed")
            
            # View/test existing connections
            with st.expander("View Connections", expanded=False):
                st.markdown("### Existing Connections")
                ConnectionList.render(
                    connections=self.db_manager.get_user_connections(st.session_state.user_id),
                    on_test=self.handle_test_connection
                )

    def render_connection_selector(self):
        """Render connection selector in main UI."""
        connections = self.db_manager.get_user_connections(st.session_state.user_id)
        if len(connections) > 1:  # Only show if multiple connections exist
            st.sidebar.markdown("### Switch Connection")
            ConnectionSelector.render(
                connections=connections,
                active_connection_id=st.session_state.active_connection_id,
                on_change=self.handle_connection_change
            )
