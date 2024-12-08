"""Schema editor UI component."""
import streamlit as st
import json
from typing import Optional, Dict, Any
from sqlalchemy.exc import IntegrityError
from src.database.db_manager import get_database_manager
from src.database.inspectors import InspectorFactory

class SchemaEditorUI:
    """Schema editor UI component."""
    
    def __init__(self, db_manager=None):
        """Initialize schema editor UI component."""
        self.db_manager = db_manager or get_database_manager()
        
        # Initialize session state
        if 'active_connection_id' not in st.session_state:
            st.session_state.active_connection_id = None
        if 'active_connection_name' not in st.session_state:
            st.session_state.active_connection_name = None
        if 'selected_table' not in st.session_state:
            st.session_state.selected_table = None
        if 'selected_db_type' not in st.session_state:
            st.session_state.selected_db_type = None

    def create_initial_schema_config(self, connection_id: str, connection_config: Dict) -> bool:
        """Create initial schema configuration for a new connection."""
        try:
            # Validate configuration
            errors = InspectorFactory.validate_config(connection_config)
            if errors:
                st.error("Invalid connection configuration:")
                for error in errors:
                    st.error(f"- {error}")
                return False
            
            # Create appropriate inspector
            inspector = InspectorFactory.create_inspector(connection_config)
            
            # Get schema information
            schema_info = inspector.inspect_schema()
            
            # Save schema configuration
            return self.db_manager.update_schema_config(connection_id, schema_info)
        except Exception as e:
            st.error(f"Error creating schema configuration: {str(e)}")
            return False

    def render_add_connection(self):
        """Render the add connection form."""
        with st.expander("➕ Add New Connection"):
            # Get supported database types
            db_types = InspectorFactory.get_supported_types()
            
            # Database type selection (outside form)
            db_type = st.selectbox(
                "Database Type",
                ["Select type..."] + db_types,
                key="db_type_selector"
            )
            
            # Show connection form only if database type is selected
            if db_type != "Select type...":
                st.session_state.selected_db_type = db_type
                
                # Connection form
                with st.form("add_connection_form"):
                    conn_name = st.text_input("Connection Name")
                    
                    # Get required parameters for selected database type
                    required_params = InspectorFactory.get_required_params(db_type)
                    
                    # Initialize config
                    config = {"type": db_type}
                    
                    # Create input fields for required parameters
                    for param in required_params:
                        if param == "password":
                            value = st.text_input(
                                param.title(),
                                type="password",
                                key=f"param_{param}"
                            )
                        else:
                            value = st.text_input(
                                param.title(),
                                key=f"param_{param}"
                            )
                        if value:
                            config[param] = value
                    
                    # Submit button
                    submitted = st.form_submit_button("Add Connection")
                    if submitted:
                        if not conn_name:
                            st.warning("Please enter a connection name")
                            return
                            
                        # Validate configuration
                        errors = InspectorFactory.validate_config(config)
                        if errors:
                            st.error("Please fill in all required fields:")
                            for error in errors:
                                st.error(f"- {error}")
                            return
                        
                        # Add connection
                        with st.spinner("Creating connection and inspecting schema..."):
                            try:
                                connection_id = self.db_manager.add_connection(
                                    st.session_state.user_id,
                                    conn_name,
                                    db_type,
                                    json.dumps(config)
                                )
                                
                                if connection_id:
                                    if self.create_initial_schema_config(connection_id, config):
                                        st.success(f"Connection '{conn_name}' added successfully!")
                                        st.session_state.active_connection_id = connection_id
                                        st.session_state.active_connection_name = conn_name
                                        st.session_state.selected_table = None
                                        st.rerun()
                                    else:
                                        st.error("Failed to create schema configuration")
                                else:
                                    st.error("Failed to add connection")
                            except IntegrityError:
                                st.error(f"Connection name '{conn_name}' already exists. Please choose a different name.")
                            except Exception as e:
                                st.error(f"Failed to add connection: {str(e)}")
            else:
                st.info("Please select a database type")

    def render_connection_selector(self):
        """Render the connection selector dropdown."""
        connections = self.db_manager.get_user_connections(st.session_state.user_id)
        
        if connections:
            options = {conn['name']: conn['id'] for conn in connections}
            options = {"Select Connection": None, **options}
            
            selected_name = st.selectbox(
                "Switch Connection",
                options.keys(),
                index=0 if not st.session_state.active_connection_name else 
                      list(options.keys()).index(st.session_state.active_connection_name)
            )
            
            if selected_name != "Select Connection":
                connection_id = options[selected_name]
                if connection_id != st.session_state.active_connection_id:
                    st.session_state.active_connection_id = connection_id
                    st.session_state.active_connection_name = selected_name
                    st.session_state.selected_table = None
                    st.rerun()

    def render_business_context(self, config: Dict[str, Any], save_callback):
        """Render business context section."""
        st.subheader("Business Context")
        
        business_context = config.get("business_context", {})
        
        # Business description
        new_desc = st.text_area(
            "Business Description",
            value=business_context.get("description", ""),
            help="High-level description of the business context"
        )
        if new_desc != business_context.get("description", ""):
            business_context["description"] = new_desc
            save_callback()
        
        # Key concepts
        st.write("Key Business Concepts (one per line)")
        concepts = "\n".join(business_context.get("key_concepts", []))
        new_concepts = st.text_area(
            "Key Concepts",
            value=concepts,
            help="Enter each key business concept on a new line"
        )
        
        new_concept_list = [c.strip() for c in new_concepts.split("\n") if c.strip()]
        if new_concept_list != business_context.get("key_concepts", []):
            business_context["key_concepts"] = new_concept_list
            save_callback()

    def render_query_guidelines(self, config: Dict[str, Any], save_callback):
        """Render query guidelines section."""
        st.subheader("Query Guidelines")
        
        query_guidelines = config.get("query_guidelines", {})
        
        # Query tips
        st.write("Query Tips (one per line)")
        tips = "\n".join(query_guidelines.get("optimization_rules", []))
        new_tips = st.text_area(
            "Optimization Rules",
            value=tips,
            help="Enter each query optimization rule on a new line"
        )
        
        new_tips_list = [t.strip() for t in new_tips.split("\n") if t.strip()]
        if new_tips_list != query_guidelines.get("optimization_rules", []):
            query_guidelines["optimization_rules"] = new_tips_list
            save_callback()
        
        # Database-specific guidelines
        if "warehouse_optimization" in query_guidelines:
            st.write("Warehouse Optimization")
            sizing_rules = "\n".join(query_guidelines["warehouse_optimization"].get("sizing_rules", []))
            new_sizing = st.text_area(
                "Sizing Rules",
                value=sizing_rules,
                help="Enter each warehouse sizing rule on a new line"
            )
            
            new_sizing_list = [s.strip() for s in new_sizing.split("\n") if s.strip()]
            if new_sizing_list != query_guidelines["warehouse_optimization"].get("sizing_rules", []):
                query_guidelines["warehouse_optimization"]["sizing_rules"] = new_sizing_list
                save_callback()

    def render_table_descriptions(self, config: Dict[str, Any], save_callback):
        """Render table descriptions section."""
        st.subheader("Table & Field Descriptions")
        
        # Table selector
        tables = list(config.get("base_schema", {}).get("tables", {}).keys())
        if not tables:
            st.warning("No tables found in schema")
            return
        
        # Reset selected table if it doesn't exist in current schema
        if st.session_state.selected_table not in tables:
            st.session_state.selected_table = None
            
        # Table selection
        selected_table = st.selectbox(
            "Select Table",
            ["Select a table..."] + tables,
            index=0 if not st.session_state.selected_table else 
                  tables.index(st.session_state.selected_table) + 1
        )
        
        if selected_table == "Select a table...":
            return
            
        st.session_state.selected_table = selected_table
        
        # Get table info
        table_info = config["base_schema"]["tables"][selected_table]
        table_desc = config.get("business_context", {}).get("table_descriptions", {}).get(selected_table, {})
        
        # Table description
        new_table_desc = st.text_area(
            f"Description for {selected_table}",
            value=table_desc.get("description", ""),
            help="Describe the business purpose of this table"
        )
        if new_table_desc != table_desc.get("description", ""):
            if "table_descriptions" not in config["business_context"]:
                config["business_context"]["table_descriptions"] = {}
            if selected_table not in config["business_context"]["table_descriptions"]:
                config["business_context"]["table_descriptions"][selected_table] = {}
            config["business_context"]["table_descriptions"][selected_table]["description"] = new_table_desc
            save_callback()
        
        # Field descriptions
        st.write("Field Descriptions:")
        for field_name, field_info in table_info.get("fields", {}).items():
            col1, col2 = st.columns([1, 3])
            with col1:
                st.write(f"**{field_name}**")
                st.write(f"Type: {field_info['type']}")
                if field_info.get("primary_key"):
                    st.write("🔑 Primary Key")
                if field_info.get("foreign_key"):
                    st.write("🔗 Foreign Key")
                if field_info.get("nullable"):
                    st.write("Optional")
                else:
                    st.write("Required")
            
            with col2:
                field_desc = table_desc.get("fields", {}).get(field_name, {})
                new_field_desc = st.text_area(
                    f"Description for {field_name}",
                    value=field_desc.get("description", ""),
                    key=f"field_{selected_table}_{field_name}",
                    help="Describe the business meaning of this field"
                )
                if new_field_desc != field_desc.get("description", ""):
                    if "fields" not in config["business_context"]["table_descriptions"][selected_table]:
                        config["business_context"]["table_descriptions"][selected_table]["fields"] = {}
                    if field_name not in config["business_context"]["table_descriptions"][selected_table]["fields"]:
                        config["business_context"]["table_descriptions"][selected_table]["fields"][field_name] = {}
                    config["business_context"]["table_descriptions"][selected_table]["fields"][field_name]["description"] = new_field_desc
                    save_callback()

    def render(self):
        """Render the schema editor UI."""
        if not st.session_state.active_connection_id:
            return
        
        schema_config = self.db_manager.get_schema_config(st.session_state.active_connection_id)
        if not schema_config:
            st.warning("No schema configuration found. Please refresh the connection.")
            return
        
        config = schema_config.get('config', {})
        modified = False
        
        def save_callback():
            nonlocal modified
            modified = True
        
        # Create tabs for different sections
        tab1, tab2, tab3 = st.tabs(["Business Context", "Query Guidelines", "Table Descriptions"])
        
        with tab1:
            self.render_business_context(config, save_callback)
            
        with tab2:
            self.render_query_guidelines(config, save_callback)
            
        with tab3:
            self.render_table_descriptions(config, save_callback)
        
        # Save changes if modified
        if modified:
            if self.db_manager.update_schema_config(
                st.session_state.active_connection_id,
                config
            ):
                st.success("Changes saved successfully!")
            else:
                st.error("Failed to save changes")
