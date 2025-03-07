import pandas as pd
import streamlit as st
import os
import io
import time
import src.contact_deduplication as cdup
import src.company_deduplcation as cmp_dup
import src.util.sqllite_helper as db_manager
import src.settings.constants as const


enrichment = False
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

with col2:
    st.write(f'Welcome, *{st.session_state.name}*')
    logout = st.button("Logout")
    if logout:
        for key in st.session_state.keys():
            del st.session_state[key]
        st.switch_page("./Home.py")


st.header("Enrichment", divider="rainbow")

# Sidebar with logo
with st.sidebar:
    logo_url = "./pages/TresVista Logo-Blue background.png"
    st.image(logo_url)

# File uploader for main file
docs = st.file_uploader("Upload your CSV or Excel Files and Click on the Submit & Process Button",
                        type=['csv', 'xlsx'],
                        accept_multiple_files=False)

if docs:
    file_info = []  # List to hold file info
    file_name = docs.name
    st.session_state.file_type = file_name.split(".")[-1]
    file_size = round(docs.size / 1024, 2)

    # Check if file already exists
    # Load the file into a pandas dataframe
    df = create_dataframe(docs)
    file_name = file_name
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

    

if st.button("Run Enrichment"):
    with st.spinner("Processing..."):
        df = create_dataframe(docs)
        df_enrichment = cdup.process_duplicate_resolution(df)
        enrichment = True
        df_col, download_col = st.columns([9, 1])
        with df_col:
            st.dataframe(df_enrichment)
        with download_col:
            if enrichment:
                deduplicated_df = df_enrichment
                base_file_name = os.path.splitext(file_name)[0]
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


