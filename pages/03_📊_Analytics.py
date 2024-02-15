import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import utils as utl

utl.inject_custom_css()
    
def load_view(tool):

    st.title('📊Analytics', anchor=False)

    # Fetch the historical data
    historical = tool.historical
    df = tool.table
    
    # Count the occurrences of each operation type
    operation_counts = historical['operation'].value_counts()

    # Create a new DataFrame to store the counts
    metrics = pd.DataFrame(operation_counts).reset_index()

    metrics.columns = ['kpi', 'count']

    # Get the total number of rows in the DataFrames
    total_rows_historical = len(historical)
    total_rows_table = len(df)

    # Add a row for the total number of rows
    total_metric_historical = pd.DataFrame([['Total Rows Historical', total_rows_historical]], columns=['kpi', 'count'])
    total_metric_table = pd.DataFrame([['Total Valid Rows', total_rows_table]], columns=['kpi', 'count'])
    metrics = pd.concat([total_metric_historical, total_metric_table, metrics])
    

    def calculate_and_format_percentage(operation_name, metrics, total_rows_historical):
        # Find the row corresponding to the operation
        operation_row = metrics[metrics['kpi'] == operation_name]
        
        # Calculate the percentage for the operation
        operation_percentage = (operation_row['count'] / total_rows_historical) * 100

        if not operation_percentage.empty:

            operation_percentage_value = operation_percentage.iloc[0]
            
            # Format the percentage as n,nn%
            operation_percentage_formatted = '{:.2f}%'.format(operation_percentage_value)
        else:
            operation_percentage_formatted = '0%'
            
        return operation_percentage_formatted
    
    with st.container(border=True):
        col1, col2, col3, col4 = st.columns(4)        
        
        insert_percentage_formatted = calculate_and_format_percentage('insert', metrics, total_rows_historical)
        update_percentage_formatted = calculate_and_format_percentage('update', metrics, total_rows_historical)
        delete_percentage_formatted = calculate_and_format_percentage('delete', metrics, total_rows_historical)

        # Display metrics using Streamlit
        col2.metric('Insert %', insert_percentage_formatted)
        col3.metric('Update %', update_percentage_formatted)
        col4.metric('Delete %', delete_percentage_formatted)


        m1 = metrics[metrics['kpi'] == 'Total Rows Historical']['count'] if not metrics[metrics['kpi'] == 'Total Rows Historical'].empty else 0
        col1.metric('Total Historical Rows:', m1.values[0])
        m2 = metrics[metrics['kpi'] == 'Total Valid Rows']['count'] if not metrics[metrics['kpi'] == 'Total Valid Rows'].empty else 0
        col1.metric('Total Valid Rows:', m2.values[0])
        m2 = metrics[metrics['kpi'] == 'insert']['count'] if not metrics[metrics['kpi'] == 'insert'].empty else 0
        col2.metric('Insert Count:', m2.values[0])
        m3 = metrics[metrics['kpi'] == 'update']['count'] if not metrics[metrics['kpi'] == 'update'].empty else 0
        col3.metric('Update Count:', m3.values[0])
        m4 = metrics[metrics['kpi'] == 'delete']['count'] if not metrics[metrics['kpi'] == 'delete'].empty else 0
        col4.metric('Delete Count:', m4)
        

    historical = historical.reset_index()
    historical['date'] = historical['timestamp'].dt.to_period('M')
    historical = historical[['date','operation']].groupby(by=['date','operation']).value_counts()
    historical = pd.DataFrame(historical,columns=['count']).reset_index()
    historical = historical.pivot(index='date', columns='operation', values='count').fillna(0).reset_index()

    colors = {'insert':'#00d26a',
        'update':'#00a6ed',
        'delete':'#f70a8d'}

    data = [
        go.Bar(x=historical['date'].astype(str), y=historical[col], name=col,  marker_color=colors[col] )
        for col in historical.columns if col != 'date'
    ]

    # Creating the layout for the plot
    layout = go.Layout(
        xaxis=dict(title='Date'),
        yaxis=dict(title='Count'),
        title='Count of Operations Over Time',
        barmode='group',  # Change to 'group' for grouped bars
        legend=dict(title='kpi')
    )


    fig = go.Figure(data=data, layout=layout)
    with st.container(border=True):
        st.plotly_chart(fig, use_container_width=True)

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