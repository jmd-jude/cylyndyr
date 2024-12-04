"""Connection selector UI component."""
import streamlit as st
from typing import List, Dict, Any, Optional

class ConnectionSelector:
    @staticmethod
    def render(
        connections: List[Dict[str, Any]], 
        active_connection_id: str,
        on_change: callable
    ) -> Optional[str]:
        """
        Render connection selector dropdown.
        
        Args:
            connections: List of connection dictionaries
            active_connection_id: Currently active connection ID
            on_change: Callback function when selection changes (takes connection_id)
            
        Returns:
            Selected connection ID if changed, None otherwise
        """
        if not connections:
            st.info("No connections available")
            return None
        
        # Create display names with database and schema info
        connection_options = [
            f"{conn['name']} ({conn['config']['database']}.{conn['config']['schema']})"
            for conn in connections
        ]
        
        # Find current index
        current_index = next(
            (i for i, conn in enumerate(connections) 
             if conn['id'] == active_connection_id),
            0
        )
        
        # Create the selector
        selected_option = st.selectbox(
            "Select Connection",
            connection_options,
            index=current_index,
            help="Choose which database connection to use"
        )
        
        # Find selected connection
        selected_conn = next(
            conn for i, conn in enumerate(connections) 
            if connection_options[i] == selected_option
        )
        
        # If connection changed, trigger callback
        if selected_conn['id'] != active_connection_id:
            on_change(selected_conn['id'])
            return selected_conn['id']
        
        return None
