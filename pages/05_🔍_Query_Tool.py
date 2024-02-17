import streamlit as st
from pandasql import sqldf
import utils as utl
import pandas as pd

utl.inject_custom_css()

def load_view(tool):

    st.title('🔍Query Tool', anchor=False)

    historical = tool.historical.reset_index()
    df = tool.table.reset_index()


    # Create a form for entering an SQL query
    with st.container(border=True):
        query = st.text_area("Enter your SQL query here", height=300, help='SQLite syntax needed [(docs)](https://pypi.org/project/pandasql/) \n\nExample: \n\nSelect * from df/historical')

        col1, col2 = st.columns(2)

        # Execute the SQL query on the selected table and display results
        submitted = col1.button("Execute SQL Query", use_container_width=True)

        if submitted:
            try:
                if query:

                    result_df = sqldf(query)

                    def convert_df(df):
                        return df.to_csv().encode('utf-8')

                    csv = convert_df(result_df)
                    col2.download_button(
                        label="Download results as CSV",
                        data=csv,
                        file_name='results.csv',
                        mime='text/csv',
                        use_container_width=True
                    )

                    st.dataframe(result_df, use_container_width=True)
                    
                else:
                    st.warning("Please enter an SQL query.", icon='⚠️')
            except Exception as e:
                st.error(f"Error executing the query: {e}", icon='❌')

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