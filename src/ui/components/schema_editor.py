"""Schema editor UI components."""
import streamlit as st
from typing import Dict, Any, Optional

from src.schema_manager import SchemaManager

class SchemaEditorUI:
    def __init__(self, schema_manager: SchemaManager):
        """Initialize schema editor UI with schema manager."""
        self.schema_manager = schema_manager

    def render_business_context(self, config: Dict[str, Any], connection_id: str):
        """Render business context editor."""
        st.subheader("Business Context")
        description = st.text_area(
            "Business Description",
            value=config.get('business_context', {}).get('description', ''),
            key="business_desc"
        )
        
        # Key Concepts
        concepts = config.get('business_context', {}).get('key_concepts', [])
        concepts_text = st.text_area(
            "Key Business Concepts (one per line)",
            value='\n'.join(concepts) if concepts else '',
            key="key_concepts"
        )
        
        if st.button("Update Business Context"):
            self.schema_manager.update_business_context(
                connection_id,
                description,
                concepts_text.split('\n') if concepts_text else []
            )
            st.success("Business context updated!")

    def render_table_editor(self, config: Dict[str, Any], connection_id: str):
        """Render table description editor."""
        st.subheader("Table Descriptions")
        selected_table = st.selectbox(
            "Select Table",
            self.schema_manager.get_tables(connection_id)
        )
        
        if selected_table:
            table_desc = st.text_area(
                f"Description for {selected_table}",
                value=config['tables'][selected_table].get('description', ''),
                key=f"table_desc_{selected_table}"
            )
            
            if st.button(f"Update {selected_table} Description"):
                self.schema_manager.update_table_description(
                    connection_id,
                    selected_table, 
                    table_desc
                )
                st.success(f"Updated description for {selected_table}!")

    def render_field_editor(self, config: Dict[str, Any], connection_id: str, selected_table: str):
        """Render field description editor."""
        st.subheader("Field Descriptions")
        selected_field = st.selectbox(
            "Select Field",
            self.schema_manager.get_fields(connection_id, selected_table)
        )
        
        if selected_field:
            field_desc = st.text_area(
                f"Description for {selected_field}",
                value=config['tables'][selected_table]['fields'][selected_field].get('description', ''),
                key=f"field_desc_{selected_table}_{selected_field}"
            )
            
            if st.button(f"Update {selected_field} Description"):
                self.schema_manager.update_field_description(
                    connection_id,
                    selected_table,
                    selected_field,
                    field_desc
                )
                st.success(f"Updated description for {selected_field}!")

    def render_editor(self, config: Optional[Dict[str, Any]], connection_id: str):
        """Render the complete schema editor UI."""
        if not config:
            st.warning("No schema configuration found for this connection.")
            return

        with st.sidebar.expander("Schema Configuration", expanded=False):
            # Business Context
            self.render_business_context(config, connection_id)
            
            # Table Descriptions
            self.render_table_editor(config, connection_id)
            
            # Get selected table from the table editor
            selected_table = st.session_state.get('selected_table')
            if selected_table:
                # Field Descriptions
                self.render_field_editor(config, connection_id, selected_table)
