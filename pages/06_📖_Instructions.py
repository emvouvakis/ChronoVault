import streamlit as st
import utils as utl

utl.inject_custom_css()


tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🛠️Configuration", "📂Database", "🔄CDC", 
                                              "📊Analytics", "📥Import Data", "🔍Query Tool"])

tcol1, tcol2 = tab1.columns([1,3])
tcol2.image('./assets/images/configuration.png')
tcol1.markdown(
    """
    #### Usage:
    1. Insert AWS Cli Access Key.
    1. Insert AWS Cli Secret Access.
    1. Click Login.
    1. Select S3 bucket.
    1. Select destination folder.
    1. Finalize selections.
    """
)
tab1.markdown(
"""
> **Note:** The IAM user associated with the provided credentials must have read and write privileges in the designated S3 bucket.
""")

tab2.markdown(
    '''
    #### Usage:
    ✅ Utilize the horizontal rows interaction for effortless data insertion or update.  
    ✅ Effortlessly delete rows by pressing the Delete key or using the provided option.  
    ✅ Ensure changes are saved with a single click of the button.

    '''
)

tab2.image('./assets/images/database.png', width=850)

tab3.markdown(
    '''
    #### Usage:
    ✅ Gain a comprehensive understanding of data modifications.  
    ✅ Track changes over time to analyze trends and patterns.
    '''
              )
tab3.image('./assets/images/cdc.png', width=850)

tab4.markdown(
    '''
    #### Usage:
    ✅ Get a snapshot showcasing the volume of inserts, updates, and deletes.  
    ✅ View a monthly Operations Chart for extra perspective on operational trends.
    '''
)
tab4.image('./assets/images/analytics.png',width=650)

tcol1, tcol2 = tab5.columns([0.3,0.6])
tcol1.markdown(
    '''
    #### Usage:
    ✅ Select local file.  
    ✅ Import data.

    '''
)
tcol2.image('./assets/images/import.png',width=600)

tcol1, tcol2 = tab6.columns([0.3,0.6])
tcol1.markdown(
    '''
    #### Usage:
    ✅ Execute SQL queries with PandasSQL.  
    ✅ Export results to csv.

    '''
)
tcol2.image('./assets/images/query.png',width=600)