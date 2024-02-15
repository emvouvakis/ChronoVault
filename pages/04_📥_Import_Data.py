import pandas as pd
import streamlit as st
import utils as utl

utl.inject_custom_css()

def load_view(tool):

    st.title('📥Import Data', anchor=False)

    def read_data(file):
        if file is not None:
            try:
                if file.name.endswith('.xlsx'):
                    temp = pd.read_excel(file, index_col=None)
                else:
                    st.error("File format not supported")
                    return None
                return temp
            except Exception as e:
                st.error(f"Error reading file: {e}")
                return None
            
    container = st.container(border=True)
    
    uploaded_file = container.file_uploader("Upload File", type=['xlsx'])

    imported_df = read_data(uploaded_file)

    if isinstance(imported_df, pd.DataFrame) and not 'timestamp' in imported_df.columns:
        container.write('Data Preview:')
        container.table(imported_df.head())
        col1, _ = container.columns(2)

        if col1.button('Import', use_container_width= True, type='primary') and isinstance(imported_df, pd.DataFrame) :
            try:
                tool.insert_new_data(imported_df)
                try:
                    tool.save_s3()
                    st.toast('Import successful', icon='✅')
                    st.cache_resource.clear()
                except Exception as e:
                    st.error(f"Error during saving: {e}", icon='❌')
            except Exception as e:
                st.error(f"Error during data insertion: {e}", icon='❌')

# Common error messages
login_error = 'Log in to access this feature.'
bucket_error = 'You have to select a S3 bucket.'
folder_error = 'You have to select a folder.'
empty_folder_error = 'No data found. Import data is needed.'

# Check route and conditions before executing functions
if not st.session_state.connected:
    st.warning(login_error, icon='🔒')
elif st.session_state.connected=='Yes' and not st.session_state.selected_bucket:
    st.warning(bucket_error, icon='🔒')
elif st.session_state.connected=='Yes' and st.session_state.selected_bucket and not st.session_state.selected_folder:
    st.warning(folder_error, icon='🔒')

elif 'tool' in st.session_state:        
    load_view(st.session_state.tool)