"""Login UI component."""
import streamlit as st
import hashlib
from typing import Optional, Tuple
from src.database.db_manager import DatabaseManager

class LoginUI:
    def __init__(self):
        """Initialize login UI component."""
        # Initialize session state
        if 'user_id' not in st.session_state:
            st.session_state.user_id = None
        if 'username' not in st.session_state:
            st.session_state.username = None
            
        # Initialize database manager
        self.db_manager = DatabaseManager()

    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256."""
        return hashlib.sha256(password.encode()).hexdigest()

    def create_user(self, email: str, password: str) -> Tuple[bool, Optional[str]]:
        """Create new user in database."""
        try:
            user_id, error = self.db_manager.add_user(email, self.hash_password(password))
            if user_id:
                return True, None
            return False, error or "Failed to create account"
        except Exception as e:
            print(f"Error creating user: {str(e)}")  # Debug print
            return False, "An unexpected error occurred"

    def verify_user(self, email: str, password: str) -> Optional[str]:
        """Verify user credentials and return user_id if valid."""
        user = self.db_manager.get_user(email)
        if user and user['password_hash'] == self.hash_password(password):
            return user['id']
        return None

    def render_login(self):
        """Render login/signup form."""
        st.title("Welcome to Cylyndyr")
        
        # Create tabs for Login and Sign Up
        login_tab, signup_tab = st.tabs(["Login", "Sign Up"])
        
        # Login Tab
        with login_tab:
            st.subheader("Login")
            with st.form("login_form"):
                login_email = st.text_input(
                    "Email",
                    key="login_email",
                    placeholder="Enter your email",
                    help="Your email for logging in",
                    autocomplete="email"
                )
                login_password = st.text_input(
                    "Password",
                    type="password",
                    key="login_password",
                    placeholder="Enter your password",
                    help="Your account password",
                    autocomplete="current-password"
                )
                
                if st.form_submit_button("Login", use_container_width=True, type="primary"):
                    if login_email and login_password:
                        user_id = self.verify_user(login_email, login_password)
                        if user_id:
                            st.session_state.user_id = user_id
                            st.session_state.username = login_email
                            st.rerun()
                        else:
                            st.error("Invalid email or password")
                    else:
                        st.warning("Please enter both email and password")
        
        # Sign Up Tab
        with signup_tab:
            st.subheader("Create Account")
            with st.form("signup_form"):
                new_email = st.text_input(
                    "Email",
                    key="signup_email",
                    placeholder="Choose an email",
                    help="Enter your email address",
                    autocomplete="email"
                )
                new_password = st.text_input(
                    "Password",
                    type="password",
                    key="signup_password",
                    placeholder="Choose a password",
                    help="Password must be at least 6 characters long",
                    autocomplete="new-password"
                )
                confirm_password = st.text_input(
                    "Confirm Password",
                    type="password",
                    key="confirm_password",
                    placeholder="Confirm your password",
                    help="Re-enter your password to confirm",
                    autocomplete="new-password"
                )
                
                if st.form_submit_button("Sign Up", use_container_width=True, type="primary"):
                    if new_email and new_password and confirm_password:
                        if new_password != confirm_password:
                            st.error("Passwords do not match")
                        elif len(new_password) < 6:
                            st.error("Password must be at least 6 characters long")
                        else:
                            success, error = self.create_user(new_email, new_password)
                            if success:
                                st.success("Account created successfully! Please log in.")
                            else:
                                st.error(error)
                    else:
                        st.warning("Please fill out all fields")

    def is_logged_in(self) -> bool:
        """Check if user is logged in."""
        return st.session_state.user_id is not None

    def logout(self):
        """Log out user."""
        st.session_state.user_id = None
        st.session_state.username = None
        st.rerun()
