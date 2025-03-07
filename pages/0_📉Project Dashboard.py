# Project Dashboard.py
import streamlit as st
from streamlit import session_state as ss
import src.util.sqllite_helper as db_manager
import pandas as pd
import time
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder

if 'stage' not in st.session_state:
    st.session_state.stage = 0
if 'project_name' not in st.session_state:
    st.session_state.project_name = ""
if 'project_types' not in st.session_state:
    st.session_state.project_types = ""
if 'file_name' not in st.session_state:
    st.session_state.file_name = ""
if 'file_type' not in st.session_state:
    st.session_state.file_type = ""
if 'deduplication_done' not in st.session_state:
    st.session_state.deduplication_done = False
if 'deduplicated_df' not in st.session_state:
    st.session_state.deduplicated_df = None
if 'resolution_df' not in st.session_state:
    st.session_state.resolution = None

if 'merge_df' not in st.session_state:
    st.session_state.merge_df = None
if 'deduplication_done' not in st.session_state:
    st.session_state.deduplication_done = False
if 'resolution_done' not in st.session_state:
    st.session_state.resolution = False
if 'deduplicated_df' not in st.session_state:
    st.session_state.deduplicated_df = pd.DataFrame()
if 'mergeing_done' not in st.session_state:
    st.session_state.mergeing_done = False

# Set up page configuration
st.set_page_config(page_title="Dashboard", page_icon=" ", layout="wide")

# Custom CSS for button styling
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

# Display page header and user information
col1, col2 = st.columns([9, 2])  # Adjust column widths

# Display header and username
with col1:
    st.markdown("<h1 style='text-align: center; color: blue;'>iCRM Cleansing Platform</h1>", unsafe_allow_html=True)
with col2:
    st.write(f'Welcome, *{st.session_state.name}*')
    if st.button("Logout"):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.switch_page("./Home.py")  # Redirect to home page

# Sidebar logo
with st.sidebar:
    st.image("./pages/TresVista Logo-Blue background.png")

# Define utility functions
def check_project_exists(project_name, user):
    str_sql = "SELECT COUNT(*) FROM projects WHERE project_name=? AND user=?"
    result = db_manager.select_sql(str_sql, [project_name, user])
    return result[0][0] > 0

def load_data_all():
    str_sql = "SELECT * from projects"
    table_s = db_manager.select_sql(str_sql, [])
    return pd.DataFrame(table_s, columns=["id", "project_types", "project_name", "client_name", "user", "status", "created_on", "updated_on"]).set_index("id")

def load_data_user(user_name):
    str_sql = "SELECT * from projects where user=?"
    table_s = db_manager.select_sql(str_sql, [user_name])
    return pd.DataFrame(table_s, columns=["id", "project_types", "project_name", "client_name", "user", "status", "created_on", "updated_on"]).set_index("id")

def create_project(project_types, new_project, client_name, user):
    str_sql = "INSERT INTO projects (project_types, project_name, client_name, user) VALUES (?, ?, ?, ?);"
    db_manager.execute_sql(str_sql, [project_types, new_project, client_name, user])

def set_stage(stage):
    st.session_state.stage = stage

# Initialize session state variables


# Load data based on user type
df = load_data_all() if st.session_state.name == "Admin" else load_data_user(st.session_state.name)

# Define JS for grid selection
disable_selection_js = JsCode("""
function(params) {
    if (params.data.status === 'Completed') {
        return {'backgroundColor': '#D3D3D3', 'pointerEvents': 'none', 'opacity': '0.5'};
    }
}
""")

# Stage 0: Display the project table
if st.session_state.stage == 0:
    st.write("### All Projects:")
    if st.button("New", on_click=set_stage, args=(1,)):
        pass

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    gb.configure_grid_options(getRowStyle=disable_selection_js)
    grid_options = gb.build()

    grid_response = AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=200,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True
    )

    # Get the selected row
    selected_row = grid_response.get('selected_rows', [])
    if isinstance(selected_row, pd.DataFrame):
        st.session_state.project_name = selected_row['project_name'][0]
        st.session_state.project_types = selected_row['project_types'][0]
        st.switch_page("./pages/1_🔀De -Duplication.py")

# Stage 1: Display the form to create a new project
if st.session_state.stage == 1:
    with st.form(key="my_form"):
        st.header("Create a New Project")
        
        # User inputs
        new_project = st.text_input("Enter Project Name")
        project_types = st.selectbox("Enter Project Type", ("company", "contact"))
        client_name = st.text_input("Enter Client Name")
        user = st.session_state.name

        # Handle form submission
        if st.form_submit_button("Create"):
            if new_project and client_name and user:
                if check_project_exists(new_project, user):
                    st.error(f"Project '{new_project}' already exists. Please choose a different name.")
                else:
                    create_project(project_types, new_project, client_name, user)
                    st.session_state.project_name = new_project
                    st.session_state.project_types = project_types
                    st.success(f"Project '{new_project}' created successfully!")
                    st.session_state.stage = 0
                    time.sleep(2)
                    st.switch_page("./pages/1_🔀De -Duplication.py")
            else:
                st.error("Please fill out all fields.")
