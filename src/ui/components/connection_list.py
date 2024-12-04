"""Connection list UI component."""
import streamlit as st
from typing import List, Dict, Any

class ConnectionList:
    @staticmethod
    def render(connections: List[Dict[str, Any]], on_test):
        """
        Render list of existing connections with test button.
        
        Args:
            connections: List of connection dictionaries
            on_test: Callback function for test button (takes connection_id)
        """
        st.subheader("Existing Connections")
        
        if not connections:
            st.info("No connections found")
            return
        
        # Use tabs for connections
        tabs = st.tabs([f"{conn['name']} ({conn['type']})" for conn in connections])
        
        for tab, conn in zip(tabs, connections):
            with tab:
                # Show connection details
                st.json({
                    'account': conn['config']['account'],
                    'database': conn['config']['database'],
                    'warehouse': conn['config']['warehouse'],
                    'schema': conn['config']['schema'],
                    'last_used': conn['last_used']
                })
                
                # Test connection button
                if st.button("Test Connection", key=f"test_{conn['id']}"):
                    on_test(conn['id'])
                
                # Show active status
                if conn['id'] == st.session_state.active_connection_id:
                    st.success("Currently Active")
