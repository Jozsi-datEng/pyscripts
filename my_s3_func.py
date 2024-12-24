
import boto3
from io import StringIO
import pandas as pd
from typing import Dict, List, Union

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def load_s3_csv_to_df(s_bucket: str, path_file: str, s_access_id: str, s_sec_key: str) -> Union[pd.DataFrame, None]:
    """ Load CSV file from S3 into a pandas DataFrame with authentication
    ver: 1.1
    Params:
        s_bucket (str): The name of the S3 bucket
        s_path_file (str): The file path inside the S3 bucket
        s_access_id (str) AWS credantial access id
        s_sec_key (str) AWS credantial access key
    Return:
        (df): DataFrame with CSV content
    Requirements:
        module(s): boto3, pandas, StringIO
    """
    s3 = boto3.client("s3", aws_access_key_id=s_access_id, aws_secret_access_key=s_sec_key)
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

def load_s3_dir_csv_files(s_bucket: str, path_work: str, s_access_id: str, s_sec_key: str) -> pd.DataFrame:
    """ s3 folder downloader with authentication
    ver: 1.0
    Params:
        s_bucket (str): s3 bucket name
        path_work (str): work dir path
        s_access_id (str) AWS credantial access id
        s_sec_key (str) AWS credantial access key
    Returns:
        DataFrame: concatenated csv content
    requirements:
        module(s): boto3
    """
    s3 = boto3.client("s3", aws_access_key_id=s_access_id, aws_secret_access_key=s_sec_key)
    ls_df_data = []
    # list dir and iterate on it
    for obj in s3.list_objects(Bucket=s_bucket, Prefix=path_work)['Contents']:
        path_file = obj['Key']
        # filter folders and suffixes that not match
        fname = path_file.split(r"/")[-1].strip()
        if fname != "" and path_file.split(".")[-1] == 'csv':
            response = s3.get_object(Bucket=s_bucket, Key=path_file)
            # got the list in response
            # extract the content
            content = response['Body']
            # read in csv data
            obj_csv_data = content.read().decode("utf-8")
            df_data = pd.read_csv(StringIO(obj_csv_data), delimiter=';')
            ls_df_data.append(df_data)
            print(f'{fname} has loaded and added')
    return pd.concat(ls_df_data, ignore_index=True)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def list_s3_dir_filtered(s_bucket: str, path_work: str, s_file_sufix: str, s_access_id: str, s_sec_key: str) -> List[str]:
    """ list filtered files from an S3 bucket
    Params:
        s_bucket (str): S3 bucket name
        path_work (str): Work dir path
        s_file_sufix (str): File suffix to filter
        s_access_id (str): AWS credential access id
        s_sec_key (str): AWS credential access key
    Returns:
        List[str]: List of file keys that match the suffix
    requirements:
    module(s): boto3
    """
    s3 = boto3.client("s3", aws_access_key_id=s_access_id, aws_secret_access_key=s_sec_key)
    ls_fname = []
    # List objects in the specified budir and prefix
    response = s3.list_objects(Bucket=s_bucket, Prefix=path_work)
    for s_fname in response.get('Contents', []):
        s_key = s_fname['Key']
        # Filter folders and check suffix
        if s_key.split("/")[-1].strip() and s_key.split(".")[-1] == s_file_sufix:
            ls_fname.append(s_key)
    return ls_fname

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
