import streamlit as st
import utils as utl
import pandas as pd 

utl.inject_custom_css()


def load_view(tool):

    historical = tool.historical.sort_values(by =['timestamp'], ascending=False).reset_index()
    historical = historical.rename_axis('row')

    columns_to_move = ['operation', 'validity_flag']

    # Get the list of existing columns excluding the ones to move
    existing_columns = [col for col in historical.columns if col not in columns_to_move]

    # Rearrange the columns 'operation' and 'validity_flag' at the end
    new_order = existing_columns + columns_to_move
    historical = historical[new_order]


    def highlight_values(val):
        color_map = {
            'insert': 'background-color: #00d26a; color: #00381c; ', 
            'update': 'background-color: #00a6ed; color: #003a53',
            'delete': 'background-color: #f70a8d; color: #5d0335'
        }
        return color_map.get(val)

    # Apply the style to the DataFrame
    styled_df = historical.style.map(highlight_values, subset=['operation'])


    st.title('🔄CDC', anchor=False)
    st.dataframe(styled_df,use_container_width=True)

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
elif st.session_state.connected=='Yes' and st.session_state.selected_bucket and st.session_state.selected_folder and not isinstance(st.session_state.tool.table, pd.DataFrame):
    st.warning(empty_folder_error, icon='🔒')

elif 'tool' in st.session_state:
    load_view(st.session_state.tool)