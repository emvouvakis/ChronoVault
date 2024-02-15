import streamlit as st
from streamlit_extras.app_logo import add_logo

@st.cache_data()
def inject_custom_css():

    with open('assets/styles.css') as f:
        style = f'<style>{f.read()}</style>'
        st.markdown(style, unsafe_allow_html=True)

    add_logo('././assets/images/logo_resized.png', height=100)  