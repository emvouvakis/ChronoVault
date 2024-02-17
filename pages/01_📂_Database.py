import streamlit as st
import pandas as pd
import utils as utl

utl.inject_custom_css()

def load_view(tool):

    col1, col2 = st.columns([3,1])

    # Use anchor for custom css
    col1.title('📂Database', anchor = 'title')
    df = tool.table
    df = df.reset_index()[df.columns.difference(['timestamp'])]

    st.data_editor(
        df,
        column_order=tuple(['id']) + tuple(df.columns.difference(['id','timestamp'])),
        hide_index=True,
        num_rows="dynamic",
        key='editor',
        use_container_width=True
    )

    state = st.session_state["editor"]


    if col2.button('Save', type = 'primary', use_container_width = True, key = 'save'):

        status = False
        errors = False

        if len(state['added_rows']) >= 1:

            for row in state['added_rows']:
                if 'id' not in row.keys():
                    errors = True

            if errors:
                st.error('The `id` column is required in the provided data.')
            else:
                status = True
                try:

                    inserts = pd.DataFrame(state['added_rows'])

                    tool.insert_new_data(inserts)
                except:
                    pass
            

        if len(state['edited_rows']) >= 1:
            
            foo = df.reset_index().iloc[list(state['edited_rows'].keys())].copy()

            if errors:
                pass
            elif df['id'].isnull().sum() and not errors:
                errors = True
                st.error('The `id` column is required in the provided data.', icon='❌')       
            else:
                status = True
                updates = pd.DataFrame(state['edited_rows'])
                updates = updates.transpose()

                if 'id' in updates.columns:
                    errors = True
                    st.error('The `id` column cannot be updated.')
                else:
                    updates['timestamp'] = pd.Timestamp.now()
                    foo.update(updates)
                    tool.update_records(foo)

        if len(state['deleted_rows'])>=1:

            status = True
            tool.delete_record(df.iloc[state['deleted_rows']]['id'].tolist())

        if not errors and status:
            try:
                tool.save_s3()
                st.toast('Saved successfully', icon='✅')
                st.cache_resource.clear()
            except Exception as e:
                st.error(f"Error during saving: {e}", icon='❌')
        elif not errors and not status:
            st.warning('No changes found in the data.', icon='⚠️')


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

