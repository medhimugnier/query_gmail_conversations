import snowflake.connector
from google.cloud import storage
import zipfile
import os

# Snowflake connection parameters
snowflake_user = 'YOUR_SNOWFLAKE_USERNAME'
snowflake_password = 'YOUR_SNOWFLAKE_PASSWORD'
snowflake_account = 'YOUR_SNOWFLAKE_ACCOUNT'
snowflake_warehouse = 'YOUR_SNOWFLAKE_WAREHOUSE'
snowflake_database = 'YOUR_SNOWFLAKE_DATABASE'
snowflake_schema = 'YOUR_SNOWFLAKE_SCHEMA'

# GCS parameters
bucket_name = 'YOUR_GCS_BUCKET_NAME'
zipped_dir_name = 'YOUR_ZIPPED_DIR_NAME'
local_extract_dir = '/path/to/local/extract/directory'  # Replace with the local directory where you want to extract the ZIP contents

# Snowflake SQL query to create a stage for the GCS data
create_stage_query = f"""
CREATE OR REPLACE STAGE gmail_stage
URL = 'gcs://{bucket_name}/{zipped_dir_name}/'
CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_GCS_ACCESS_KEY'
    AWS_SECRET_KEY = 'YOUR_GCS_SECRET_KEY'
);
"""

# Snowflake SQL query to copy data from the stage to a Snowflake table
copy_query = f"""
COPY INTO your_snowflake_table
FROM @gmail_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = CONTINUE; -- You can choose how to handle errors during the copy process
"""

def extract_zip_file(zip_file_path, extract_dir):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

def main():
    # Extract the contents of the ZIP file
    zip_file_path = os.path.join(local_extract_dir, f'{zipped_dir_name}.zip')
    extract_zip_file(zip_file_path, local_extract_dir)

    # Connect to Snowflake
    connection = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

    # Create the stage for GCS data
    cursor = connection.cursor()
    cursor.execute(create_stage_query)
    cursor.close()

    # Run the COPY query to load data from the GCS stage to Snowflake table
    cursor = connection.cursor()
    cursor.execute(copy_query)
    cursor.close()

    # Close the Snowflake connection
    connection.close()

    print("Data successfully loaded into Snowflake table.")

if __name__ == "__main__":
    main()
