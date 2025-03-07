import streamlit as st
import pandas as pd
import os
import io
import time
import src.company_deduplcation as cmp_dup
import src.util.sqllite_helper as db_manager
import src.contact_deduplication as cont


# Page Configuration
st.set_page_config(page_title="Download", page_icon="📈", layout="wide")
# Function to create DataFrame based on file type
def create_dataframe(file_path: str):
    if st.session_state.file_type == "csv":
        df = pd.read_csv(file_path, index_col=False)
    elif st.session_state.file_type == "xlsx":
        df = pd.read_excel(file_path, engine='openpyxl', index_col=False)
    return df
# Hide deploy button in Streamlit
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

# Save uploaded files to directory
def save_uploaded_file(uploaded_file):
    save_dir = "data_files"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
# Ensure the output directory exists
output_dir = "output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Get the base file name without extension
base_file_name = os.path.splitext(st.session_state.file_name)[0]


# Create the new file name with timestamp
new_file_name = f"{base_file_name}_Resolution"

# Check the file type and save the resolution DataFrame accordingly
# Check the file type and save the resolution DataFrame accordingly
if st.session_state.file_type == "csv":
    resolution_file_path = os.path.join(output_dir, f"{new_file_name}.csv")
    if not os.path.exists(resolution_file_path):
        st.session_state.resolution.to_csv(resolution_file_path, index=False)
elif st.session_state.file_type == "xlsx":
    resolution_file_path = os.path.join(output_dir, f"{new_file_name}.xlsx")
    if not os.path.exists(resolution_file_path):
        with pd.ExcelWriter(resolution_file_path, engine='xlsxwriter') as writer:
            st.session_state.resolution.to_excel(writer, index=False)
# Header and Project Information
col1, col2 = st.columns([9, 2])
with col1:
    st.markdown(
        """<h1 style='text-align: center; color: blue;'>iCRM Cleansing Platform</h1>""",
        unsafe_allow_html=True
    )
    st.write(f'Project: {st.session_state.project_name}')
    st.write(f'Project Type: {st.session_state.project_types}')
with col2:
    st.write(f'Welcome, *{st.session_state.get("name", "User")}*')
    logout = st.button("Logout")
    if logout:
        for key in st.session_state.keys():
            del st.session_state[key]
        st.switch_page("./Home.py")

# Page Title
st.header("Resolution of Duplicate Records")

# Sidebar with logo
with st.sidebar:
    st.image("./pages/TresVista Logo-Blue background.png")
resolution_df = st.session_state.resolution
resolution_df = resolution_df.drop(columns=["FirstName","LastName","GroupID","IsDuplicate","new_title","new_phone","new_group_id"])
# Display the DataFrame in the main area
df_col, download_col = st.columns([9, 1])
with df_col:
    st.dataframe(resolution_df)
with download_col:
    if st.session_state.resolution_done:
        base_file_name = os.path.splitext(st.session_state.file_name)[0]
        new_file_name = f"{base_file_name}_Resolution"

        if st.session_state.file_type == "csv":
            csv = resolution_df.to_csv(index=False).encode('utf-8')
            with download_col:
                st.download_button(label="Download", data=csv, file_name=f"{new_file_name}.csv", mime="text/csv")
        elif st.session_state.file_type == "xlsx":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                resolution_df.to_excel(writer, index=False)
            excel_data = output.getvalue()
            with download_col:
                st.download_button(label="Download", data=excel_data, file_name=f"{new_file_name}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


    
# File uploader for main file
docs = st.file_uploader("Upload your Data File",
                        type=['csv', 'xlsx'],
                        accept_multiple_files=False)
if docs:
    edited_df = create_dataframe(docs)    
# Display edited DataFrame and merge button

if st.button('Merging', type="primary"):
    merge_records_df = edited_df.copy()
    if st.session_state.get("project_types") == 'company':
        st.session_state.merge_df = cmp_dup.merge_records(merge_records_df)
    else:
        st.session_state.merge_df = cont.merge_records(merge_records_df)
    
    st.session_state.mergeing_done = True
    st.switch_page("pages/4_🔁Merge Records.py")
