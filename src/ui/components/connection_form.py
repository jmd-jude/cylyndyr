"""Connection form UI component."""
import streamlit as st
from typing import Optional, Dict

class ConnectionForm:
    @staticmethod
    def render() -> Optional[Dict[str, str]]:
        """Render form for adding a new connection. Returns credentials if submitted."""
        st.subheader("Add New Connection")
        with st.form("new_connection_form"):
            name = st.text_input("Connection Name", placeholder="My Snowflake Connection")
            account = st.text_input("Account", placeholder="your-account")
            user = st.text_input("Username")
            password = st.text_input("Password", type="password")
            database = st.text_input("Database")
            warehouse = st.text_input("Warehouse")
            schema = st.text_input("Schema")
            
            submitted = st.form_submit_button("Add Connection")
            
            if submitted:
                if not all([name, account, user, password, database, warehouse, schema]):
                    st.error("All fields are required")
                    return None
                
                # Remove .snowflakecomputing.com if present
                if '.snowflakecomputing.com' in account:
                    account = account.replace('.snowflakecomputing.com', '')
                
                return {
                    'name': name,
                    'credentials': {
                        'account': account,
                        'user': user,
                        'password': password,
                        'database': database,
                        'warehouse': warehouse,
                        'schema': schema
                    }
                }
            
            return None
