import pandas as pd
import streamlit as st
import os
import io
import time
import src.contact_deduplication as cdup
import src.company_deduplcation as cmp_dup
import src.util.sqllite_helper as db_manager
import src.settings.constants as const

# Set the page configuration
st.set_page_config(page_title="Data Upload", page_icon=" ", layout="wide")

# Custom CSS for buttons
st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #0099ff;
        color:#ffffff;
    }
    .stDeployButton {
        visibility: hidden;
    }
    </style>
    """, unsafe_allow_html=True)

# Query previously uploaded files
query = """
    SELECT file_name, user_name, file_type, file_size, records
    FROM project_files
    WHERE project_name = ? AND user_name = ? AND project_types = ?
"""
uploaded_files = db_manager.select_sql(query, [
    st.session_state.project_name,
    st.session_state.get('name', 'Anonymous'),
    st.session_state.project_types
])

# Function to check if a project file already exists
def check_project_exists(file_name, user):
    str_sql = "SELECT COUNT(*) FROM project_files WHERE file_name = ? AND user_name = ?"
    result = db_manager.select_sql(str_sql, [file_name, user])
    return result[0][0] > 0

# Function to save uploaded files
def save_uploaded_file(uploaded_file):
    save_dir = "data_files"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    file_path = os.path.join(save_dir, uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

# Function to create DataFrame based on file type
def create_dataframe(file_path: str):
    if st.session_state.file_type == "csv":
        df = pd.read_csv(file_path, index_col=False)
    elif st.session_state.file_type == "xlsx":
        df = pd.read_excel(file_path, engine='openpyxl', index_col=False)
    return df

# Create columns for layout


col1, col2 = st.columns([9, 2])
with col1:
    st.markdown("<h1 style='text-align: center; color: blue;'>iCRM Cleansing Platform</h1>", unsafe_allow_html=True)
    st.write(f'Project: {st.session_state.project_name}')
    st.write(f'Project Type: {st.session_state.project_types}')

with col2:
    st.write(f'Welcome, *{st.session_state.name}*')
    logout = st.button("Logout")
    if logout:
        for key in st.session_state.keys():
            del st.session_state[key]
        st.switch_page("./Home.py")

# Display previously uploaded files, if any
if uploaded_files:
    st.subheader("Previously Uploaded Files")
    prev_files_df = pd.DataFrame(uploaded_files, columns=["FileName", "User Name", "File Type", "File Size", "Records"])
    prev_files_df_styled = prev_files_df.style.apply(
        lambda x: ["background-color: lightgray" if x.name % 2 == 0 else "" for i in x], axis=1
    )
    st.dataframe(prev_files_df_styled, hide_index=True)

st.header("De-Duplication", divider="rainbow")

# Sidebar with logo
with st.sidebar:
    logo_url = "./pages/TresVista Logo-Blue background.png"
    st.image(logo_url)

# File uploader for main file
docs = st.file_uploader("Upload your Data File",
                        type=['csv', 'xlsx'],
                        accept_multiple_files=False)

if docs:
    file_info = []  # List to hold file info
    file_name = docs.name
    st.session_state.file_type = file_name.split(".")[-1]
    file_size = round(docs.size / 1024, 2)

    # Check if file already exists
    file_exist = db_manager.select_scaler("SELECT * FROM project_files WHERE file_name = ?", [file_name])

    # Load the file into a pandas dataframe
    df = create_dataframe(docs)
    st.session_state.file_name = file_name
    num_records = len(df) if df is not None else 0
    file_info.append({
        "FileName": file_name,
        "UserName": st.session_state.get('name', 'Anonymous'),
        "FileType": st.session_state.file_type,
        "FileSize": f"{file_size} KB",
        "Records": num_records
    })

    file_df = pd.DataFrame(file_info)
    styled_df = file_df.style.apply(lambda x: ["background-color: lightblue" if x.name % 2 == 0 else "" for i in x], axis=1)

    # Insert file information if it doesn't already exist
    if not check_project_exists(file_name, st.session_state.get('name', 'Anonymous')):
        file_info_query = """
            INSERT INTO project_files (project_types, project_name, file_name, user_name, file_type, file_size, records)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        db_manager.execute_sql(file_info_query, [
            st.session_state.project_types, st.session_state.project_name,
            file_name, st.session_state.get('name', 'Anonymous'),
            st.session_state.file_type, f"{file_size} KB", num_records
        ])

    # Display file information with download button next to it
    df_col, download_col = st.columns([9, 1])
    with df_col:
        st.dataframe(styled_df, hide_index=True)
        map_file = st.file_uploader("Upload your Mapping Files and Click on the Run Deduplication Button",
                                    type=['csv', 'xlsx'],
                                    accept_multiple_files=False)

# Deduplication functionality

if st.button("Run Deduplication"):
    with st.spinner("Processing..."):
        file_path = save_uploaded_file(docs)
        map_path = save_uploaded_file(map_file) if map_file else None

        # Run the deduplication based on project type
        if st.session_state.project_types == 'company':
            st.session_state.deduplicated_df = cmp_dup.process_company_duplicator(file_path, map_path)
        else:
            st.session_state.deduplicated_df = cdup.process_contact_deduplication(file_path, map_path)

        st.session_state.deduplication_done = True
        time.sleep(4)
        st.switch_page("./pages/2_📄Download_file.py")

