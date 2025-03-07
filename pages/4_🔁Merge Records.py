import streamlit as st
import pandas as pd
import os
import io
import src.company_deduplcation as cmp_dup
import src.util.sqllite_helper as db_manager
import src.contact_deduplication as cont

# Page Configuration
st.set_page_config(page_title="Merge Records", page_icon="📈", layout="wide")

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
# Hide deploy button in Streamlit
st.markdown(
    r"""
    <style>
    .stDeployButton {
        visibility: hidden;
    }
    </style>
    """, 
    unsafe_allow_html=True
)

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
st.header("Merge Records")

# Sidebar with logo
with st.sidebar:
    st.image("./pages/TresVista Logo-Blue background.png")
merge_df= st.session_state.merge_df
# Display the DataFrame in the main area
df_col, download_col = st.columns([9, 1])
with df_col:
    st.dataframe(merge_df)
with download_col:
    if st.session_state.deduplication_done:
        base_file_name = os.path.splitext(st.session_state.file_name)[0]
        new_file_name = f"{base_file_name}_merged"

        if st.session_state.file_type == "csv":
            csv = merge_df.to_csv(index=False).encode('utf-8')
            with download_col:
                st.download_button(label="Download", data=csv, file_name=f"{new_file_name}.csv", mime="text/csv")
                str_sql = "UPDATE projects SET status= ? WHERE project_name= ? "
                db_manager.execute_sql(str_sql, ("Completed", st.session_state.project_name))
                st.session_state.deduplication_done = False
        elif st.session_state.file_type == "xlsx":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                merge_df.to_excel(writer, index=False)
            excel_data = output.getvalue()
            with download_col:
                st.download_button(label="Download", data=excel_data, file_name=f"{new_file_name}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                str_sql = "UPDATE projects SET status= ? WHERE project_name= ? "
                db_manager.execute_sql(str_sql, ("Completed", st.session_state.project_name))
                st.session_state.deduplication_done = False

# Navigation to Next Pag
if st.button(' Go To Dashboard', type="primary"):
    for key in list(st.session_state.keys()):
        if key != "name":
            del st.session_state[key]
    st.switch_page("./pages/0_📉Project Dashboard.py")
    