# import json
import os
import boto3
from io import StringIO
import pandas as pd
from typing import Dict, Union

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def load_s3_csv_to_df(s_bucket: str, path_file: str, s_access_id: str, s_sec_key: str) -> Union[pd.DataFrame, None]:
    """ Load CSV file from S3 into a pandas DataFrame with autentication
    ver: 1.1
    Params:
        s_bucket (str): The name of the S3 bucket
        s_path_file (str): The file path inside the S3 bucket
        s_access_id = AWS credantial access id
        s_sec_key = AWS credantial access key
    Return:
        (df): DataFrame with CSV content
    Requirements:
        module(s): boto3, pandas, StringIO
    """
    s3 = boto3.client("s3", aws_access_key_id=s_access_id, aws_secret_access_key=s_sec_key)
   
    # s3 = boto3.client("s3")
    try:
        # Get the object from S3
        obj_csv = s3.get_object(Bucket=s_bucket, Key=path_file)
        obj_csv_data = obj_csv["Body"].read().decode("utf-8")
        # Read the CSV data into a pandas DataFrame
        df_data = pd.read_csv(StringIO(obj_csv_data), delimiter=';')
        return df_data
    except Exception as e:
        print(f"Error: {str(e)}")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
