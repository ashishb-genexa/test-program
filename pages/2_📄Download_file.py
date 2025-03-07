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
st.header("De_Duplication of Records")

# Sidebar with logo
with st.sidebar:
    st.image("./pages/TresVista Logo-Blue background.png")
deduplicated_df = st.session_state.deduplicated_df
deduplicated_df = deduplicated_df.drop(columns=["FirstName","LastName","GroupID","Email","CompanyName","Designation","IsDuplicate","new_title","new_phone","new_email","prv_org","prv_title","new_group_id"])
# Display the DataFrame in the main area
df_col, download_col = st.columns([9, 1])
with df_col:
    st.dataframe(deduplicated_df)
with download_col:
    if st.session_state.deduplication_done:
        base_file_name = os.path.splitext(st.session_state.file_name)[0]
        new_file_name = f"{base_file_name}_deduplicated"

        if st.session_state.file_type == "csv":
            csv = deduplicated_df.to_csv(index=False).encode('utf-8')
            with download_col:
                st.download_button(label="Download", data=csv, file_name=f"{new_file_name}.csv", mime="text/csv")
        elif st.session_state.file_type == "xlsx":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                deduplicated_df.to_excel(writer, index=False)
            excel_data = output.getvalue()
            with download_col:
                st.download_button(label="Download", data=excel_data, file_name=f"{new_file_name}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Navigation to Next Page
if st.button('Resolution', type="primary"):
    st.session_state.resolution = cont.process_duplicate_resolution(st.session_state.deduplicated_df)
    st.session_state.resolution_done = True
    time.sleep(2)
    st.switch_page("./pages/3_🕵️‍♂️Human_Review.py")
    