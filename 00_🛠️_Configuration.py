import streamlit as st
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import utils as utl
from pathlib import Path
from tool import S3CDC

@st.cache_resource(show_spinner=False)
def connect_s3(access_key, secret_key):
    if access_key and secret_key:
        try:
            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            buckets_df = pd.DataFrame(s3.list_buckets()['Buckets'])
            print('Connect to S3')

            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            buckets_df = None
            if error_code == 'InvalidAccessKeyId':
                st.error(f"Invalid AWS Access Key ID: {error_message}")
            elif error_code == 'InvalidSecretAccessKey':
                st.error(f"Invalid AWS Secret Access Key: {error_message}")
            else:
                st.error(f"An error occurred: {error_code} - {error_message}")
        finally:
            return s3, buckets_df
    
def list_subfolders(bucket_name, s3, prefix=''):
    
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )

    subfolders = [common_prefix.get('Prefix').replace("/","") for common_prefix in response.get('CommonPrefixes', [])]
    
    return subfolders



st.set_page_config(layout="wide", page_title='ChronoVault', page_icon=Path('./assets/images/logo.png').read_bytes(), menu_items=None)
utl.inject_custom_css()

st.title('🛠️Configuration', anchor=False)
col1, col2 = st.columns(2)

keys_to_initialize = ["access_key", "secret_key", "connected", "buckets_df", "s3", "selected_folder", "selected_bucket"]

for key in keys_to_initialize:
    if key not in st.session_state:
        st.session_state[key] = None

placeholder = col1.empty()
c1 = placeholder.container(border=True)

access_key1 = c1.text_input("Enter Access Key:", type='password', key='access_key')
secret_key1 = c1.text_input("Enter Secret Key:", type='password', key ='secret_key')

subcol1 ,subcol2 = c1.columns(2)


def connect():
    try:
        st.session_state["s3"] , st.session_state["buckets_df"] = connect_s3(access_key1, secret_key1)
        st.session_state["connected"] = "Yes"
    except:
        st.session_state["connected"] = None
        pass

login = subcol1.button('Login', use_container_width=True,type='primary', on_click=connect)

if isinstance(st.session_state["buckets_df"], pd.DataFrame):
    buckets_df = st.session_state.buckets_df
    

    if st.session_state["connected"]=='Yes':
        disconnect = subcol2.button('Logout', use_container_width=True)

        if disconnect :
            print('Disconnect')
            st.session_state.clear()
            buckets_df = None
            placeholder.empty()
            placeholder.write('Thanks for using me ')
            st.toast('Logout successful', icon='👋')
            st.rerun()


        if isinstance(buckets_df, pd.DataFrame):
            c2 = col2.container(border=True)
            subcol3 , subcol4 = c2.columns(2)
                
            selected_bucket = st.session_state["selected_bucket"] if "selected_bucket" in st.session_state else None
            index = buckets_df[buckets_df['Name'] == selected_bucket].index.tolist()[0] if selected_bucket not in ["",None] else None
            selected_bucket = subcol3.selectbox("Available buckets:", buckets_df['Name'],index= index)

            if selected_bucket!=st.session_state["selected_bucket"]:
                st.session_state["selected_folder"]=None

            st.session_state["selected_bucket"] = selected_bucket if selected_bucket else None

            print(f'Connected: {st.session_state["connected"]}')
                

            if st.session_state["selected_bucket"]:
                subfolders = list_subfolders(selected_bucket, st.session_state["s3"])

                st.session_state["selected_bucket"] = selected_bucket

                if "selected_bucket" in st.session_state and len(subfolders)==0:
                    subcol4.error('No Folders found in S3.', icon='❌')   

                

                if len(subfolders)>0 or st.session_state["selected_folder"]:

                    selected_folder = st.session_state["selected_folder"]
                    selected_folder = selected_folder+"/" if selected_folder else None

                    subfolder_index = subfolders.index(selected_folder.replace('/','')) if selected_folder else None
                    selected_folder = subcol4.selectbox("Available Folders:", subfolders, index = subfolder_index)

                    def finalize():
                        if selected_folder:
                            with c2.status('Status'):
                                st.session_state["selected_folder"] = selected_folder
                                st.write('Loading data...')
                                st.session_state.tool = S3CDC(st.session_state["access_key"], st.session_state["secret_key"],
                                                                st.session_state["selected_bucket"], st.session_state["selected_folder"])
                                if isinstance(st.session_state.tool.table,pd.DataFrame):
                                    st.write('Data loaded ✅')
                                else:
                                    st.write('Data not found ❌')

                    c2.button('Select Folder', use_container_width=True, on_click=finalize)