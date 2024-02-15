## Overview

**ChronoVault** manages Change Data Capture (CDC) operations using pandas, AWS S3 storage and Streamlit.

<br/>

To get started localy follow these steps:



1. **Cloning the Repository:**

    Clone the project repository using the following Git command:

    ```bash
    git clone https://github.com/emvouvakis/ChronoVault.git
    ```

2. **Creating and Activating Conda Environment:**

    a. Create a Conda environment by executing:

    ```bash
    conda create --name env python=3.10
    ```

    b. Activate the newly created environment:

    ```bash
    conda activate env
    ```

    c. Install requirements:

    ```bash
    pip install -r requirements.txt
    ```

3. **Executing the Script:**

    ```bash
    streamlit run 00_🛠️_Configuration.py
    ```

<br/>


## Functionalities

1. **Data Management:**
   - Handles insertion, updating, deletion, and validation of data.

2. **Storage and Retrieval:**
   - Saves and retrieves data to/from an AWS S3 bucket as Parquet files.

<br/>


## ChronoVault App


<br/>

| Sheet Name       | Functionality                                               |
|------------------|-------------------------------------------------------------|
|🛠️Configuration| - AWS Login: Authenticate with AWS using your credentials.<br> - Select Bucket and Folder: Choose an AWS S3 bucket and folder to save data.|
|📥Import Data| - Upload an Excel or CSV file for data insertion.|
|📂Database| - Streamlit Data Editor: Use Streamlit's interactive table to insert, update, and delete data.  |
|🔄CDC| - Historical Table: View the historical table containing the historical data.  |
|📊Analysis| - Metrics: Display metrics about the number of inserts, updates, and deletes over time.   |
|🔍Query Tool| - Execute Queries: Utilize PandasSQL to execute queries on the DataFrame and historical table.  |

