import streamlit as st
import requests
from urllib.parse import urlencode
import base64

# Spotify API credentials
CLIENT_ID = 'your_client_id'
CLIENT_SECRET = 'your_client_secret'
REDIRECT_URI = 'http://localhost:8501/oauth-callback'

# Spotify API endpoints
AUTH_URL = 'https://accounts.spotify.com/authorize'
TOKEN_URL = 'https://accounts.spotify.com/api/token'

def get_auth_url():
    params = {
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI,
        'scope': 'user-read-private user-read-email'  # Add more scopes as needed
    }
    return f"{AUTH_URL}?{urlencode(params)}"

def exchange_code_for_token(code):
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI
    }
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    return response.json()

# Streamlit app structure
st.title('Spotify OAuth Example')

# Check if we're on the callback route
if 'code' in st.experimental_get_query_params():
    code = st.experimental_get_query_params()['code'][0]
    token_info = exchange_code_for_token(code)
    st.success("Successfully authenticated!")
    st.json(token_info)
else:
    st.write("Click the button below to authenticate with Spotify")
    if st.button('Authenticate with Spotify'):
        auth_url = get_auth_url()
        st.markdown(f"[Click here to authenticate]({auth_url})")
