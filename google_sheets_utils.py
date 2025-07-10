import os
import time
import json
import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

# Scope for Google Sheets (read-only)
SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def sheets_authenticate():
    """Authenticate with Google Sheets API for both local and Streamlit Cloud"""
    creds = None
    
    # Check for existing token in session state
    if "sheets_token_info" in st.session_state:
        try:
            creds = Credentials.from_authorized_user_info(
                st.session_state.sheets_token_info, SHEETS_SCOPES)
            if creds and creds.valid:
                return build("sheets", "v4", credentials=creds)
            elif creds and creds.expired and creds.refresh_token:
                st.info("Refreshing Google Sheets credentials...")
                creds.refresh(Request())
                st.session_state.sheets_token_info = json.loads(creds.to_json())
                st.info("Sheets token refreshed successfully.")
                return build("sheets", "v4", credentials=creds)
        except Exception as e:
            st.error(f"Failed to use cached token: {str(e)}")
            if "sheets_token_info" in st.session_state:
                del st.session_state["sheets_token_info"]
    
    # Load credentials from Streamlit secrets or credentials.json
    creds_data = None
    if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
        try:
            creds_data = json.loads(st.secrets["google"]["credentials_json"])
        except json.JSONDecodeError:
            st.error("Error: st.secrets['google']['credentials_json'] is not valid JSON.")
            return None
    elif os.path.exists('credentials.json'):
        try:
            with open('credentials.json', 'r') as f:
                creds_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            st.error(f"Error loading credentials.json: {str(e)}")
            return None
    else:
        st.error("Google credentials missing. Configure st.secrets or add credentials.json.")
        return None
    
    # Determine redirect URI based on environment
    if st.runtime.exists():
        # Running on Streamlit Cloud
        redirect_uri = f"https://{os.environ['STREAMLIT_SERVER_BASE_URL']}/"
    else:
        # Running locally
        redirect_uri = "http://localhost:8501/"
    
    # Configure OAuth flow
    flow = Flow.from_client_config(
        client_config=creds_data,
        scopes=SHEETS_SCOPES,
        redirect_uri=redirect_uri
    )
    
    # Generate authorization URL
    auth_url, _ = flow.authorization_url(prompt='consent')
    
    # Check for callback code
    if "code" in st.query_params:
        try:
            code = st.query_params["code"]
            flow.fetch_token(code=code)
            creds = flow.credentials
            
            # Save credentials in session state
            st.session_state.sheets_token_info = json.loads(creds.to_json())
            st.success("Successfully connected to Google Sheets API!")
            
            # Clear code from URL
            st.experimental_set_query_params()
            return build("sheets", "v4", credentials=creds)
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}. Please try again.")
            st.experimental_set_query_params()
            return None
    else:
        st.markdown("### Google Sheets Authentication Required")
        st.warning("This app requires access to your Google Sheets to fetch data. Please authorize.")
        st.markdown(f"""
        1. [Authorize with Google Sheets]({auth_url})
        2. You'll be redirected back to the app automatically.
        """)
        st.stop()
    
    return None