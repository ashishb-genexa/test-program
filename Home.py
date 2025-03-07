# Import necessary libraries
import streamlit_authenticator as stauth
import streamlit as st
import yaml
from yaml.loader import SafeLoader
import sqlite3
import pandas as pd
import src.settings.constants as const
import os

if not os.path.exists('./output'):
    os.makedirs('./output')
if not os.path.exists('./data'):
    os.makedirs('./data')
# Clear session state on page load
# st.session_state.clear()  # Uncomment if you need to clear session state on each load
if 'name' not in st.session_state:
    st.session_state.name = ''

# Set up Streamlit page configuration
st.set_page_config(page_title='Streamlit', page_icon=' ', initial_sidebar_state='collapsed')

# Establish SQLite database connection
conn = sqlite3.connect(const.DB_FILE_PATH)
c = conn.cursor()

# Create 'users_data' table if it doesn't exist
c.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT
    )
''')
conn.commit()

# Function to check login credentials
def login_user(username, password):
    c.execute('SELECT * FROM users WHERE username= ? AND password = ?', (username, password))
    return c.fetchone()

# Layout: Split page into two columns
col1, col2 = st.columns(2)

# Add image to the first column
with col1:
    st.image("Login Page.png", caption="Your Image Caption")

# Add login form to the second column
with col2.container(height=435):
    st.title('Login Page')
    username = st.text_input('Username')
    password = st.text_input('Password', type='password')

    # Login button and action
    if st.button('Login'):
        user = login_user(username, password)
        if user:
            st.success('Login successful!')
            st.session_state.name = username
            st.switch_page("pages/0_📉Project Dashboard.py")
        else:
            st.error('User ID or password is incorrect')

# Close the database connection after login check
conn.close()

# Custom CSS styling for layout enhancement
st.markdown(
    """
    <style>
    .css-1e5imcs {
        display: flex;
        align-items: stretch;
    }
    .css-1e5imcs > div {
        flex: 1;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .css-1e5imcs > div > div {
        flex: 1;
    }
    </style>
    """,
    unsafe_allow_html=True
)