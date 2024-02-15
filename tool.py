import pandas as pd
import streamlit as st

# @st.cache_resource(show_spinner=False)
class S3CDC:

    def __init__(self,aws_access_key_id, aws_secret_access_key, aws_bucket, aws_folder_in_bucket):
        """
        Initializes the DaskS3CDC class.
        Handles a Dask DataFrame for change data capture operations.

        Args:
        - new_data: Optional parameter to provide initial data.
        """
        self.table=None
        if aws_access_key_id and aws_secret_access_key and aws_bucket and aws_folder_in_bucket:

            # AWS credentials and S3 bucket information
            self.aws_access_key_id = aws_access_key_id
            self.aws_secret_access_key = aws_secret_access_key
            self.bucket = aws_bucket
            self.aws_folder_in_bucket = aws_folder_in_bucket
            

            # Initializing
            self.historical = pd.DataFrame()
            self.isnerts = pd.DataFrame()
            self.updates = pd.DataFrame()
            self.deletes = pd.DataFrame()
            self.error = None
            
            try:
                if not self.error:
                    self.read_s3()
            except Exception as e:
                self.error = True

    def insert_new_data(self, new_data:pd.DataFrame):
        """
        Inserts new data into the Dask DataFrame.
        Marks insert records in the historical dataframe.

        Args:
        - new_data: DataFrame to be inserted.
        """
        if 'id' not in new_data.columns:
            raise ValueError("The 'id' column is required in the provided data.")

        if sum( new_data.duplicated(subset=['id'], keep=False) ) > 1:
            raise ValueError("All 'id' values must be unique.")
        
        if 'timestamp' in new_data.columns:
            raise ValueError("The 'timestamp' column should be renamed.") 
        

        # Add current timestamp 
        new_data['timestamp'] = pd.Timestamp.now()

        for col in new_data.columns:
            if col!= 'timestamp':

                timestamp_columns = [col for col in new_data.columns.difference(['timestamp']) if pd.to_datetime(new_data[col], errors='coerce', format='%Y-%m-%dT%H:%M:%S.%f').notnull().all()]
                new_data[timestamp_columns] = new_data[timestamp_columns].apply(pd.to_datetime, errors='coerce', format='%Y-%m-%dT%H:%M:%S.%f')

        # Set timestamp as index
        new_data = new_data.reset_index(drop=True)
        new_data.loc[:,('operation')] = 'insert'
        new_data = new_data.set_index('timestamp')

        self.isnerts = new_data

        # Update dataframes with the new data
        if self.historical is not None and len(self.historical) != 0:
            
            if new_data['id'].isin(self.historical['id']).any():
                raise ValueError("The 'ids' already exists.")
            
            self.historical = pd.concat([new_data, self.historical])
        else:
            self.historical = new_data

        self.validate_data()
        


    def update_records(self, _updates):
        """
        Updates specific records in the DataFrame based on provided ID and column values.
        Marks updated records in the historical dataframe.

        Args:
        - id_column_values: Dictionary containing columns and their respective updated values.
        """

        updates = _updates.set_index('id')
        for col in updates.columns:
            if col!= 'timestamp':

                timestamp_columns = [col for col in updates.columns.difference(['timestamp']) if pd.to_datetime(updates[col], errors='coerce', format='%Y-%m-%dT%H:%M:%S.%f').notnull().all()]
                updates[timestamp_columns] = updates[timestamp_columns].apply(pd.to_datetime, errors='coerce', format='%Y-%m-%dT%H:%M:%S.%f')

        
        self.historical = self.historical.reset_index().set_index('timestamp')

        updated = self.historical[(self.historical['id'].isin(_updates['id'].values) ) & (self.historical['validity_flag'] == 1)]
        updated.loc[:,('operation')] = 'update'
        
        updated = updated.reset_index().set_index('id')
        updated.loc[:,('timestamp')] = pd.Timestamp.now()
        
        updated.update(updates)
        updated = updated.reset_index().set_index('timestamp')

        self.updates = updated.drop(columns=['validity_flag'])

        self.historical = pd.concat([updated, self.historical])

        self.validate_data()

    def delete_record(self, target_ids: list = None):
        """
        Deletes specified records by ID, marking them as 'delete' in the historical dataframe.

        Args:
        - target_ids: List of IDs to be deleted.
        """
        if target_ids is not None and not isinstance(target_ids, list):
            raise TypeError("target_ids must be a list")

        for target_id in target_ids:            
            specific_row = self.historical[
                (self.historical['id'] == target_id) & 
                (self.historical['validity_flag'] == 1)
            ]
            
            specific_row = specific_row.reset_index().set_index('id')
            deletes = pd.DataFrame({'operation':'delete','timestamp':pd.Timestamp.now()}, index=[target_id]).rename_axis('id')
            specific_row.update(deletes)
            specific_row = specific_row.reset_index().set_index('timestamp')
            
            # Update self.historical by appending specific_row
            self.historical = pd.concat([specific_row, self.historical])

            self.deletes = specific_row.drop(columns=['validity_flag'])
            
            self.validate_data()

    def validate_data(self):
        """
        Validates the data by setting a 'validity_flag' based on the maximum timestamp per ID.
        """
        self.historical = self.historical.reset_index()
        
        self.historical = self.historical.sort_values(by=['id','timestamp'], ascending=False).set_index('id')

        # Group by 'id' and find the maximum timestamp within each group
        max_timestamp_per_id = self.historical.groupby('id')['timestamp'].transform('max')

        # Set 'validity_flag' to 1 for rows that have the maximum timestamp for their respective ID
        self.historical['validity_flag'] = (self.historical['timestamp'] == max_timestamp_per_id).astype(int)

        self.historical = self.historical.reset_index().set_index('timestamp')


        self.table = self.historical.loc[(self.historical['validity_flag'] == True) & (self.historical['operation'] != 'delete')
                                         , ~self.historical.columns.isin(['validity_flag', 'operation'])]


    def save_s3(self):
        """
        Saves dataframes to S3 bucket as Parquet files using only Pandas.
        Implements parallel computation for faster data storage.
        Uses partitioning to organize data by year and month.
        """

        duplicated_historic = pd.concat([self.isnerts , self.updates, self.deletes])
        
        # Extract year and month from the timestamp index
        duplicated_historic['year'] = pd.to_datetime(duplicated_historic.index).year
        duplicated_historic['month'] = pd.to_datetime(duplicated_historic.index).month
        
        # Reset the index to access 'timestamp' as a column
        duplicated_historic = duplicated_historic.reset_index()
        
        # Set 'timestamp' as the index again
        duplicated_historic = duplicated_historic.set_index('timestamp')
        
        # Define function for saving to S3 with partitioning
        def _save_s3(data_frame, bucket, table):
            print(f"Data saved: s3://{bucket}/{table}/")
            data_frame.to_parquet(
                f"s3://{bucket}/{table}/",
                partition_cols=['year', 'month'],
                engine='pyarrow',
                compression="SNAPPY",
                storage_options={
                    'key': self.aws_access_key_id,
                    'secret': self.aws_secret_access_key 
                }
            )
        
        # Save duplicated_historic to S3 in parallel
        _save_s3(duplicated_historic, self.bucket, self.aws_folder_in_bucket)

        self.isnerts , self.updates, self.deletes = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def read_s3(self):
        """
        Reads data from S3 bucket and initializes dataframes using only Pandas.
        """
        print('Reading S3 data')
        
        # Define function for reading from S3
        def _read_s3(path):
            # Read Parquet files from S3 path into a Pandas DataFrame
            df = pd.read_parquet(
                f"s3://{self.bucket}/{self.aws_folder_in_bucket}/",
                engine='pyarrow',
                storage_options={
                    'key': self.aws_access_key_id,
                    'secret': self.aws_secret_access_key
                }
            ).drop(columns=['year', 'month']).reset_index().set_index('timestamp')
            
            
            return df

        # Read data from S3 using Pandas
        self.historical = _read_s3(f"s3://{self.bucket}/{self.aws_folder_in_bucket}/")

        self.validate_data()

