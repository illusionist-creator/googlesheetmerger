import streamlit as st
import pandas as pd
import requests
import re
import io
from datetime import datetime
from typing import List, Dict, Optional
import time
import os
import json

# For Google API integration
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

# --- Google API Constants and Authentication Setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
CREDENTIALS_FILE = 'credentials.json'

def authenticate_google_sheets_oauth():
    """Authenticate with Google Sheets API using Streamlit interface."""
    creds = None
    query_params = st.query_params

    # Determine redirect URI - prioritize secrets, then detect from current URL
    redirect_uri = None
    
    # First check secrets
    if "google" in st.secrets and "redirect_uri" in st.secrets["google"]:
        redirect_uri = st.secrets["google"]["redirect_uri"]
    else:
        # Auto-detect based on current environment
        # In Streamlit Cloud, we can detect the URL from the browser
        # For production, always use the Streamlit app URL
        if "streamlit.app" in st.secrets.get("general", {}).get("app_url", ""):
            redirect_uri = st.secrets["general"]["app_url"]
        elif hasattr(st, 'get_option') and 'server.headless' in st.get_option('server.headless', True):
            # Running in Streamlit Cloud
            redirect_uri = "https://ggl-sheet-merger.streamlit.app/"
        else:
            # Local development
            redirect_uri = "http://localhost:8501/"
    
    # Fallback to your production URL if nothing else works
    if not redirect_uri:
        redirect_uri = "https://ggl-sheet-merger.streamlit.app/"
        
    # Check for existing token in session state
    if "sheets_token_info" in st.session_state:
        try:
            creds = Credentials.from_authorized_user_info(
                st.session_state.sheets_token_info, SCOPES)
            if creds and creds.valid:
                return build("sheets", "v4", credentials=creds)
            elif creds and creds.expired and creds.refresh_token:
                st.info("Refreshing Google Sheets credentials...")
                creds.refresh(Request())
                st.session_state.sheets_token_info = json.loads(creds.to_json())
                return build("sheets", "v4", credentials=creds)
        except Exception as e:
            st.error(f"Failed to use cached token: {str(e)}")
            if "sheets_token_info" in st.session_state:
                del st.session_state["sheets_token_info"]

    # Load credentials from credentials.json first, then st.secrets
    creds_data = None
    if os.path.exists(CREDENTIALS_FILE):
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                creds_data = json.load(f)
        except json.JSONDecodeError:
            st.error(f"Error: {CREDENTIALS_FILE} is not a valid JSON file. Please check its content.")
            return None
    elif "google" in st.secrets and "credentials_json" in st.secrets["google"]:
        try:
            creds_data = json.loads(st.secrets["google"]["credentials_json"])
        except json.JSONDecodeError:
            st.error("Error: st.secrets['google']['credentials_json'] is not a valid JSON string.")
            return None
    else:
        st.error(f"Google credentials missing. Please provide '{CREDENTIALS_FILE}' in the app directory or configure st.secrets['google']['credentials_json'].")
        return None

    # Configure OAuth flow
    flow = Flow.from_client_config(
        client_config=creds_data,
        scopes=SCOPES,
        redirect_uri=redirect_uri
    )

    # Generate authorization URL
    auth_url, _ = flow.authorization_url(prompt='consent')

    # Check for callback code
    if "code" in query_params:
        try:
            code = query_params["code"]
            flow.fetch_token(code=code)
            creds = flow.credentials

            # Save credentials in session state
            st.session_state.sheets_token_info = json.loads(creds.to_json())
            st.success("Google Sheets authentication successful!")
            
            # Clear the code from URL to prevent re-authentication loop
            st.query_params.clear()
            st.rerun()
            return build("sheets", "v4", credentials=creds)
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}. Please try again.")
            return None
    else:
        st.markdown("### 🔐 Google Sheets Authentication Required")
        st.markdown(f"""
        **Current redirect URI:** `{redirect_uri}`
        
        1. [**Authorize with Google**]({auth_url})
        2. You'll be redirected back to this app
        3. If you encounter issues, ensure your Google Cloud Console has the correct redirect URI configured
        """)
        
        # Add debug info
        with st.expander("🔧 Debug Information"):
            st.write(f"**Redirect URI being used:** {redirect_uri}")
            st.write("**Make sure this URI is configured in your Google Cloud Console:**")
            st.write("1. Go to Google Cloud Console → APIs & Services → Credentials")
            st.write("2. Edit your OAuth 2.0 Client ID")
            st.write("3. Add the redirect URI shown above to 'Authorized redirect URIs'")
        
        st.stop()

    return None

# --- GoogleSheetsCombiner Class ---
class GoogleSheetsCombiner:
    def __init__(self):
        self.sheets_data = []
        self.combined_data = pd.DataFrame()
        self.sheets_service = None 
        
    def set_sheets_service(self, service):
        self.sheets_service = service

    def extract_sheet_id(self, url: str) -> str:
        """Extract Google Sheet ID from URL."""
        pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
        match = re.search(pattern, url)
        
        if not match:
            raise ValueError(f"Invalid Google Sheets URL format: {url}")
        
        return match.group(1)
    
    def get_sheet_info(self, sheet_id: str) -> List[Dict]:
        """Get information about all sheets in a Google Sheets document."""
        sheets_info = []
        
        # Try Google Sheets API first if authenticated
        if self.sheets_service:
            try:
                spreadsheet_metadata = self.sheets_service.spreadsheets().get(
                    spreadsheetId=sheet_id,
                    fields='sheets.properties'
                ).execute()
                
                for sheet in spreadsheet_metadata.get('sheets', []):
                    prop = sheet.get('properties', {})
                    sheets_info.append({
                        "gid": str(prop.get('sheetId')),
                        "name": prop.get('title')
                    })
                
                if sheets_info:
                    st.success(f"✅ Found {len(sheets_info)} sheets using Google Sheets API")
                    return sheets_info
                else:
                    st.warning("No sheets found via API.")
                    return []
                    
            except HttpError as err:
                if err.resp.status == 403:
                    st.error("❌ Access denied. Please ensure you have 'Viewer' access to this sheet")
                    return []
                elif err.resp.status == 404:
                    st.error(f"❌ Spreadsheet not found. Please check the URL.")
                    return []
                else:
                    st.error(f"❌ API error: HTTP {err.resp.status}")
                    return []
                    
            except Exception as e:
                st.error(f"❌ Unexpected API error: {e}")
                return []
        
        # Fallback to public parsing if not authenticated
        if not self.sheets_service:
            try:
                url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                # Try multiple patterns to extract sheet information
                patterns = [
                    r'"sheetName":"([^"]+)","sheetId":(\d+)',
                    r'"title":"([^"]+)","sheetId":(\d+)',
                    r'"name":"([^"]+)","sheetId":(\d+)',
                    r'{"properties":{"sheetId":(\d+),"title":"([^"]+)"'
                ]
                
                for pattern in patterns:
                    if pattern == r'{"properties":{"sheetId":(\d+),"title":"([^"]+)"':
                        matches = re.findall(pattern, response.text)
                        for gid, name in matches:
                            sheets_info.append({"gid": str(gid), "name": name})
                    else:
                        matches = re.findall(pattern, response.text)
                        for name, gid in matches:
                            sheets_info.append({"gid": str(gid), "name": name})
                    
                    if sheets_info:
                        break
                
                if sheets_info:
                    st.success(f"✅ Found {len(sheets_info)} sheets using public URL parsing")
                    return sheets_info
                else:
                    st.warning("⚠️ No sheets found. This may be a private sheet requiring authentication.")
                    return [{"gid": "0", "name": "Sheet1"}]
                    
            except requests.exceptions.RequestException as e:
                if "401" in str(e) or "Unauthorized" in str(e):
                    st.error("❌ This sheet is private and requires authentication.")
                    st.info("💡 Please use the 'Connect to Google Sheets' button to authenticate.")
                else:
                    st.error(f"❌ Failed to fetch sheet info: {e}")
                return []
            except Exception as e:
                st.error(f"❌ Unexpected error: {e}")
                return []
        
        st.error("❌ Unable to access this sheet. Please check authentication and permissions.")
        return []
        
    def fetch_sheet_data(self, sheet_id: str, sheet_name: str = None, gid: str = "0", header_row: int = 1) -> pd.DataFrame:
        """
        Fetch sheet data, prioritizing Google Sheets API if authenticated.
        Always returns a pandas DataFrame (empty if no data or error).
        """
        if self.sheets_service:
            try:
                # Get all data from the sheet using A1 notation
                range_name = f"'{sheet_name}'" if sheet_name else "Sheet1"
                
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    majorDimension='ROWS'
                ).execute()
                
                values = result.get('values', [])
                if not values:
                    st.warning(f"⚠️ Sheet '{sheet_name or 'Sheet1'}' is empty.")
                    return pd.DataFrame()
                
                # Create DataFrame with proper handling of varying row lengths
                max_cols = max(len(row) for row in values) if values else 0
                
                # Pad all rows to have the same length
                padded_values = []
                for row in values:
                    padded_row = row + [''] * (max_cols - len(row))
                    padded_values.append(padded_row)
                
                # Use specified header row (convert to 0-indexed)
                header_idx = header_row - 1
                if header_idx >= len(padded_values):
                    st.error(f"Header row {header_row} is beyond the data range. Using row 1 instead.")
                    header_idx = 0
                
                headers = padded_values[header_idx] if header_idx < len(padded_values) else []
                
                # Handle empty or duplicate headers
                unique_headers = []
                header_count = {}
                for i, header in enumerate(headers):
                    if not header or header.strip() == '':
                        header = f'Column_{i+1}'
                    if header in header_count:
                        header_count[header] += 1
                        unique_headers.append(f"{header}_{header_count[header]}")
                    else:
                        header_count[header] = 0
                        unique_headers.append(header)
                
                # Use data starting from the row after headers
                data_start_idx = header_idx + 1
                data = padded_values[data_start_idx:] if data_start_idx < len(padded_values) else []
                
                df = pd.DataFrame(data, columns=unique_headers)
                df['_source_sheet'] = sheet_name or f"Sheet_{sheet_id}_gid{gid}"
                
                return df
                
            except HttpError as err:
                if err.resp.status == 403:
                    st.error(f"❌ Access denied to sheet '{sheet_name or 'Sheet1'}'. Please check your permissions.")
                elif err.resp.status == 404:
                    st.error(f"❌ Sheet '{sheet_name or 'Sheet1'}' not found.")
                else:
                    st.error(f"❌ API Error: HTTP {err.resp.status}")
                return pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error fetching sheet '{sheet_name or 'Sheet1'}': {str(e)}")
                return pd.DataFrame()
        
        # Fallback to public CSV export
        if not self.sheets_service:
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
            
            try:
                response = requests.get(csv_url, timeout=30)
                response.raise_for_status()
                
                if response.text.strip().startswith('<!DOCTYPE html>'):
                    raise ValueError("Sheet may not be publicly accessible. Received HTML instead of CSV.")
                
                df = pd.read_csv(io.StringIO(response.text), header=header_row-1)
                
                # Ensure unique column names
                unique_headers = []
                header_count = {}
                for i, header in enumerate(df.columns):
                    header_str = str(header) if pd.notnull(header) else f'Column_{i+1}'
                    if header_str in header_count:
                        header_count[header_str] += 1
                        unique_headers.append(f"{header_str}_{header_count[header_str]}")
                    else:
                        header_count[header_str] = 0
                        unique_headers.append(header_str)
                
                df.columns = unique_headers
                df['_source_sheet'] = sheet_name or f"Sheet_{sheet_id}_gid{gid}"
                
                return df
                
            except requests.exceptions.RequestException as e:
                if "401" in str(e) or "Unauthorized" in str(e):
                    st.error(f"❌ Sheet '{sheet_name or 'Sheet1'}' is private and requires authentication.")
                    st.info("💡 Please use the 'Connect to Google Sheets' button to authenticate.")
                else:
                    st.error(f"❌ Failed to fetch sheet data: {str(e)}")
                return pd.DataFrame()
            except pd.errors.EmptyDataError:
                st.warning(f"⚠️ Sheet '{sheet_name or 'Sheet1'}' is empty.")
                return pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error processing sheet '{sheet_name or 'Sheet1'}': {str(e)}")
                return pd.DataFrame()
        
        st.error(f"❌ Unable to fetch data for sheet '{sheet_name or 'Sheet1'}'.")
        return pd.DataFrame()

    def add_sheet(self, sheet_id: str, sheet_info: Dict, header_row: int = 1, custom_name: str = None) -> bool:
        """Add a single sheet to internal state."""
        sheet_name = sheet_info['name']
        gid = sheet_info['gid']
        
        try:
            df = self.fetch_sheet_data(sheet_id=sheet_id, sheet_name=sheet_name, gid=gid, header_row=header_row)
            if df is not None and not df.empty:
                # Use custom name if provided, otherwise use original sheet name
                display_name = custom_name if custom_name else sheet_name
                
                self.sheets_data.append({
                    "id": sheet_id,
                    "gid": gid,
                    "name": sheet_name,
                    "display_name": display_name,
                    "data": df,
                    "header_row": header_row
                })
                return True
            else:
                st.warning(f"⚠️ Sheet '{sheet_name}' is empty or inaccessible.")
                return False
        except Exception as e:
            st.error(f"❌ Error adding sheet '{sheet_name}': {str(e)}")
            return False

    def combine_sheets(self) -> pd.DataFrame:
        """Combine all added sheets into a single DataFrame."""
        if not self.sheets_data:
            raise ValueError("No sheets added yet.")
        
        combined = pd.concat([sheet["data"] for sheet in self.sheets_data], ignore_index=True)
        self.combined_data = combined
        return self.combined_data

    def get_summary(self) -> Dict:
        """Return a summary of the combined data."""
        try:
            columns = list(self.combined_data.columns)
            total_rows = len(self.combined_data)
            total_columns = len(columns)

            sheet_stats = []
            for sheet in self.sheets_data:
                sheet_stats.append({
                    "name": sheet["display_name"],
                    "rows": len(sheet["data"]),
                    "header_row": sheet["header_row"]
                })

            return {
                "columns": columns,
                "total_rows": total_rows,
                "total_columns": total_columns,
                "sheets": sheet_stats
            }
        except Exception as e:
            return {"error": str(e)}

# Initialize session state
if 'combiner' not in st.session_state:
    st.session_state.combiner = GoogleSheetsCombiner()
if 'available_sheets_info' not in st.session_state:
    st.session_state.available_sheets_info = []
if 'last_sheet_url_input' not in st.session_state:
    st.session_state.last_sheet_url_input = ""
if 'google_sheets_service' not in st.session_state:
    st.session_state.google_sheets_service = None
if 'auth_initiated' not in st.session_state:
    st.session_state.auth_initiated = False
if 'show_advanced_options' not in st.session_state:
    st.session_state.show_advanced_options = False

def main():
    st.set_page_config(
        page_title="Google Sheets Combiner",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
    }
    .main-header h1 {
        color: white;
        margin: 0;
        font-size: 2.5rem;
    }
    .main-header p {
        color: #f0f0f0;
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
    }
    .metric-container {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #667eea;
        margin: 0.5rem 0;
    }
    .success-box {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .warning-box {
        background: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .info-box {
        background: #e7f3ff;
        border: 1px solid #bee5eb;
        color: #0c5460;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .sheet-card {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stButton > button {
        border-radius: 8px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    .stSelectbox {
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📊 Google Sheets Combiner</h1>
        <p>Combine multiple Google Sheets into a single file with ease</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🔧 Configuration")
        
        # Authentication Section
        with st.expander("🔐 Google Authentication", expanded=True):
            if st.session_state.google_sheets_service is None:
                st.info("Connect to access private sheets")
                if st.button("🔗 Connect to Google Sheets", type="primary", use_container_width=True):
                    st.session_state.auth_initiated = True
                
                if st.session_state.auth_initiated:
                    service = authenticate_google_sheets_oauth()
                    if service:
                        st.session_state.google_sheets_service = service
                        st.session_state.combiner.set_sheets_service(service)
                        st.success("✅ Connected to Google Sheets API!")
                        st.session_state.auth_initiated = False
                        st.rerun()
            else:
                st.success("✅ Connected to Google Sheets API")
                if st.button("🔌 Disconnect", type="secondary", use_container_width=True):
                    st.session_state.google_sheets_service = None
                    st.session_state.combiner.set_sheets_service(None)
                    if "sheets_token_info" in st.session_state:
                        del st.session_state.sheets_token_info
                    st.info("🔌 Disconnected from Google Sheets API")
                    st.rerun()
        
        st.markdown("---")
        
        # Add New Sheet Section
        st.markdown("### 📥 Add New Sheet")
        
        current_sheet_url_input = st.text_input(
            "Google Sheets URL", 
            placeholder="https://docs.google.com/spreadsheets/d/...",
            help="Paste the full Google Sheets URL here"
        )
        
        if current_sheet_url_input and current_sheet_url_input != st.session_state.last_sheet_url_input:
            try:
                sheet_id = st.session_state.combiner.extract_sheet_id(current_sheet_url_input)
                with st.spinner("🔍 Fetching available sheets..."):
                    st.session_state.available_sheets_info = st.session_state.combiner.get_sheet_info(sheet_id)
                st.session_state.last_sheet_url_input = current_sheet_url_input
                
                if not st.session_state.available_sheets_info:
                    st.error("❌ No sheets found. Please check the URL and permissions.")
                    
            except ValueError as e:
                st.error(f"❌ Invalid URL: {e}")
                st.session_state.available_sheets_info = []
            except Exception as e:
                st.error(f"❌ Error fetching sheets: {e}")
                st.session_state.available_sheets_info = []
        
        # Sheet Selection via Dropdown
        if st.session_state.available_sheets_info:
            st.markdown("#### 📋 Select Sheet")
            sheet_names = [sheet['name'] for sheet in st.session_state.available_sheets_info]
            selected_sheet = st.selectbox(
                "Choose a sheet to add",
                options=sheet_names,
                key="sheet_selector",
                help="Select a sheet from the Google Sheets document"
            )
            
            # Configuration for selected sheet
            selected_sheet_info = next((sheet for sheet in st.session_state.available_sheets_info if sheet['name'] == selected_sheet), None)
            if selected_sheet_info:
                col1, col2 = st.columns(2)
                
                with col1:
                    custom_name = st.text_input(
                        "Custom Name (optional)",
                        value=selected_sheet,
                        key="custom_name_selected",
                        help="Give this sheet a custom name in the combined data"
                    )
                
                with col2:
                    header_row = st.number_input(
                        "Header Row",
                        min_value=1,
                        max_value=10,
                        value=1,
                        key="header_row_selected",
                        help="Which row contains the column headers?"
                    )
                
                if st.button(f"➕ Add '{selected_sheet}'", key="add_selected_sheet", type="primary", use_container_width=True):
                    with st.spinner("📥 Adding sheet..."):
                        success = st.session_state.combiner.add_sheet(
                            sheet_id=st.session_state.combiner.extract_sheet_id(current_sheet_url_input),
                            sheet_info=selected_sheet_info,
                            header_row=header_row,
                            custom_name=custom_name if custom_name != selected_sheet else None
                        )
                    
                    if success:
                        st.success(f"✅ Successfully added '{custom_name if custom_name != selected_sheet else selected_sheet}'!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning(f"⚠️ Failed to add sheet '{selected_sheet}'. It may be empty or inaccessible.")

        elif current_sheet_url_input:
            st.warning("⚠️ No sheets available. This may be a private sheet requiring authentication.")
        
        st.markdown("---")
        
        # Management Section
        st.markdown("### 🗂️ Management")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Clear All", type="secondary", use_container_width=True):
                st.session_state.combiner = GoogleSheetsCombiner()
                if st.session_state.google_sheets_service:
                    st.session_state.combiner.set_sheets_service(st.session_state.google_sheets_service)
                st.session_state.available_sheets_info = []
                st.session_state.last_sheet_url_input = ""
                st.success("🗑️ All sheets cleared!")
                st.rerun()
        
        with col2:
            if st.button("🔄 Refresh", type="secondary", use_container_width=True):
                st.rerun()
    
    # Main content area
    if st.session_state.combiner.sheets_data:
        # Show added sheets
        st.markdown("## 📋 Added Sheets")
        
        if not st.session_state.combiner.sheets_data:
            st.info("👉 No sheets added yet. Use the sidebar to add a sheet.")
        else:
            for i, sheet in enumerate(st.session_state.combiner.sheets_data):
                with st.container():
                    st.markdown(f"""
                    <div class="sheet-card">
                        <h4>📄 {sheet['display_name']}</h4>
                        <p><strong>Rows:</strong> {len(sheet['data'])} | <strong>Header Row:</strong> {sheet['header_row']}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    col1, col2, col3 = st.columns([3, 1, 1])
                    
                    with col1:
                        if st.button(f"👁️ Preview Data", key=f"preview_{i}"):
                            st.dataframe(sheet['data'].head(10), use_container_width=True)
                    
                    with col2:
                        if st.button(f"📊 Info", key=f"info_{i}"):
                            st.info(f"**Sheet:** {sheet['name']}\n**Rows:** {len(sheet['data'])}\n**Columns:** {len(sheet['data'].columns)}")
                    
                    with col3:
                        if st.button(f"🗑️ Remove", key=f"remove_{i}", type="secondary"):
                            st.session_state.combiner.sheets_data.pop(i)
                            st.success(f"🗑️ Removed '{sheet['display_name']}'")
                            st.rerun()
        
        st.markdown("---")
        
        # Combine and Download Section
        st.markdown("## 🔄 Combine & Download")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            disable_combine = len([s for s in st.session_state.combiner.sheets_data if not s['data'].empty]) == 0
            if st.button("🔄 Combine All Sheets", type="primary", use_container_width=True, disabled=disable_combine):
                with st.spinner("🔄 Combining sheets..."):
                    try:
                        combined_df = st.session_state.combiner.combine_sheets()
                        st.success(f"✅ Successfully combined {len(combined_df)} rows from {len(st.session_state.combiner.sheets_data)} sheets!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error combining sheets: {str(e)}")
        
        with col2:
            # File naming
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"combined_sheets_{timestamp}"
            
            filename = st.text_input(
                "Output Filename (without extension)",
                value=default_filename,
                help="Enter the filename for your combined data"
            )
        
        # Show combined data if available
        if not st.session_state.combiner.combined_data.empty:
            st.markdown("## 📊 Combined Data Preview")
            
            # Summary metrics
            summary = st.session_state.combiner.get_summary()
            if "error" not in summary:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Rows", summary['total_rows'])
                with col2:
                    st.metric("Total Columns", summary['total_columns'])
                with col3:
                    st.metric("Source Sheets", len(summary['sheets']))
            
            # Data preview
            st.dataframe(st.session_state.combiner.combined_data, use_container_width=True)
            
            # Download buttons
            st.markdown("### 💾 Download Options")
            
            col1, col2 = st.columns(2)
            
            with col1:
                csv_buffer = io.StringIO()
                st.session_state.combiner.combined_data.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                
                st.download_button(
                    label="📄 Download as CSV",
                    data=csv_data,
                    file_name=f"{filename}.csv",
                    mime="text/csv",
                    type="primary",
                    use_container_width=True
                )
            
            with col2:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    st.session_state.combiner.combined_data.to_excel(writer, sheet_name='Combined Data', index=False)
                excel_data = excel_buffer.getvalue()
                
                st.download_button(
                    label="📊 Download as Excel",
                    data=excel_data,
                    file_name=f"{filename}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True
                )
            
            # Advanced options
            with st.expander("🔧 Advanced Options"):
                st.markdown("### 📋 Column Information")
                if summary and "columns" in summary:
                    st.write("**Available Columns:**")
                    for col in summary["columns"]:
                        st.write(f"- {col}")
                
                st.markdown("### 📈 Sheet Statistics")
                if summary and "sheets" in summary:
                    for sheet_stat in summary["sheets"]:
                        st.write(f"- **{sheet_stat['name']}**: {sheet_stat['rows']} rows (Header: Row {sheet_stat['header_row']})")
    else:
        # Welcome message when no sheets are added
        st.markdown("""
        <div class="info-box">
            <h3>👋 Welcome to Google Sheets Combiner!</h3>
            <p>Get started by adding your first Google Sheet:</p>
            <ol>
                <li>🔗 <strong>Optional:</strong> Connect to Google Sheets for private sheet access</li>
                <li>📝 Paste a Google Sheets URL in the sidebar</li>
                <li>📋 Select a sheet from the dropdown</li>
                <li>⚙️ Configure header row and custom name</li>
                <li>➕ Add the sheet to your combination</li>
                <li>🔄 Combine and download your data!</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
        
        # Feature highlights
        st.markdown("## ✨ Features")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            ### 🔐 Authentication Support
            - Access private Google Sheets
            - Secure OAuth 2.0 authentication
            - Works with public sheets too
            
            ### 📊 Smart Data Handling
            - Automatic column alignment
            - Duplicate header resolution
            - Source sheet tracking
            """)
        
        with col2:
            st.markdown("""
            ### 🎯 Flexible Configuration
            - Custom sheet names
            - Configurable header rows
            - Easy sheet selection
            
            ### 💾 Multiple Export Formats
            - CSV download
            - Excel (.xlsx) export
            - Data preview before download
            """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>📊 <strong>Google Sheets Combiner</strong> | Built with Streamlit</p>
        <p><small>Combine multiple Google Sheets into one file with ease</small></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()