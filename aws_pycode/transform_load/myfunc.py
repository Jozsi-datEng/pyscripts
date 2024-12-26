import json
import boto3
from io import StringIO
import pandas as pd
from typing import Dict, Union

s3 = boto3.client("s3")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def load_s3_dir_files(s_bucket: str, path_work: str, s_file_sufix: str) -> Dict:
    """ s3 folder downloader. Dowload files form specified folder
    ver: 1.0
    Params:
        s_bucket (str): s3 bucket name
        path_work (str): work dir path
    Returns:
        dict: found records {filename: file}
    requirements:
        module(s): boto3, json
    """
    dic_res = {}
    for s_fname in s3.list_objects(Bucket=s_bucket, Prefix=path_work)['Contents']:
        s_key = s_fname['Key']
        if s_key.split("/")[-1].strip() != "":
            if s_key.split(".")[1] == s_file_sufix:
                response = s3.get_object(Bucket=s_bucket, Key=s_key)
                content = response['Body']
                js_data = json.loads(content.read())
                dic_res.update({s_key.split("/")[-1].strip(): js_data})
    return dic_res

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def load_s3_csv_to_df(s_bucket: str, path_file: str) -> Union[pd.DataFrame, None]:
    """Load CSV file from S3 into a pandas DataFrame
    ver: 1.0
    Params:
        s_bucket (str): The name of the S3 bucket
        s_path_file (str): The file path inside the S3 bucket
    Return:
        (df): DataFrame with CSV content
    Requirements:
        module(s): boto3, pandas, StringIO
    """
    s3 = boto3.client("s3")
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

def save_df_to_s3(bucket: str, path_file: str, df_data: pd.DataFrame) -> Union[str, None]:
    """Save a DataFrame to S3 as a CSV file
    ver: 1.0
    Params:
        path_file (str): The full file path, including the filename
        df_data (df): The DataFrame to be saved to S3
        bucket (str): The name of the S3 bucket
    Return:
        str: message
    Requirements:
        module(s): boto3, pandas
    """
    s3 = boto3.client("s3") 
    try:
        # Convert DataFrame to CSV in memory
        buffer = StringIO()
        df_data.to_csv(buffer, sep=";", index=False)
        buffer.seek(0)
        # Save the CSV to S3
        s3.put_object(Bucket=bucket, Key=path_file, Body=buffer.getvalue())
        print (f"File saved {path_file} successfully")
    except Exception as e:
        print (f"Error: {str(e)}")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def move_file_on_s3(bucket: str, path_file: str, path_file_new: str) -> None:
    """Move file in S3 (copy and then delete original)
    ver: 1.0
    Params:
        path_file (str): full file path in the source S3 bucket
        path_file_new (str): full file path with new filename in the destination S3 bucket
        bucket (str): name of the S3 bucket
    return: message (str)
    requirements:
        module(s): boto3
    """
    s3 = boto3.client("s3")
    # Copy the file to the new location
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": path_file},
        Key=path_file_new
    )
    # Delete the original file (not always necessary, but it will be general)
    s3.delete_object(
        Bucket=bucket,
        Key=path_file
    )
    print (f"File moved from {path_file} to {path_file_new}")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def trasform_daily_wheater_data(js_weather_data: Dict) -> pd.DataFrame:
    """ filter, process daily wheater data
    ver: 1.0
    Params:
        weather_data (json): wheater data to pocessing
    Returns:
        df: processed wheater data
    requirements:
        module(s): pandas
    """
    # selected data to dataFrame
    di_weather_data_filt = {
        "datetime": js_weather_data["hourly"]["time"],
        "tempretaure": js_weather_data["hourly"]["temperature_2m"],
        "wind_speed": js_weather_data["hourly"]["wind_speed_10m"],
        "rain": js_weather_data["hourly"]["rain"],
        "precipitation": js_weather_data["hourly"]["precipitation"]
    }
    df_wether_data = pd.DataFrame(di_weather_data_filt)
    # convert type to datetime
    df_wether_data['datetime'] = pd.to_datetime(df_wether_data['datetime'])
    return df_wether_data
    
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def trasform_taxi_data(df_taxi_data: pd.DataFrame) -> pd.DataFrame:
    """ taxi_data_transformations
    ver: v1.1
    Params:
        df_taxi_data (df): original daly taxi data
    Returns:
        df: cleaned & trasform taxi data
    requirements:
        module(s): pandas
    """
    # drop unnecessary columns
    df_taxi_data.drop(["pickup_census_tract", "dropoff_census_tract"], axis=1, inplace=True)
    df_taxi_data.drop(["pickup_centroid_location", "dropoff_centroid_location"], axis=1, inplace=True)
    # drop rows that have missings
    df_taxi_data.dropna (inplace=True)
    # renaming cols
    di_col_old_new = {
        "pickup_community_area": "pickup_community_area_id", "dropoff_community_area": "dropoff_community_area_id"}
    df_taxi_data.rename (columns=di_col_old_new, inplace=True)
    # create helper column
    df_taxi_data["datetime_for_weather"] = pd.to_datetime(df_taxi_data["trip_start_timestamp"])
    # time rounding to date
    df_taxi_data["datetime_for_weather"] = df_taxi_data["datetime_for_weather"].dt.floor("h")
    # create dict with types def
    di_df_col_type = {
        "trip_id": "object",
        "taxi_id": "object",
        "trip_start_timestamp": "datetime64[ns]",
        "trip_end_timestamp": "datetime64[ns]",
        "trip_seconds": "int32",
        "trip_miles": "float64",
        "pickup_community_area_id": "int8",
        "dropoff_community_area_id": "int8",
        "fare": "float64",
        "tips": "float64",
        "tolls": "float64",
        "extras": "float64",
        "trip_total": "float64",
        "payment_type": "object",
        "company": "object",
        "pickup_centroid_latitude": "object",
        "pickup_centroid_longitude": "object",
        "dropoff_centroid_latitude": "object",
        "dropoff_centroid_longitude": "object",
        "datetime_for_weather": "datetime64[ns]"
    }
    # apply to dataFrame
    df_taxi_data = df_taxi_data.astype(di_df_col_type)
    return df_taxi_data

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def master_table_update(df_master: pd.DataFrame, df_dim: pd.DataFrame, s_id_name: str, s_column_label: str) -> pd.DataFrame:
    """ compare master and dim table. Expand master with new items if necessary.
    ver: 1.0
    Params:
        df_master (df): master data table
        df_dim (df): Actual dim (extract) table from taxi data
        s_id_name (str): master table id column name
        s_column_label (str): master table label column name
    Returns:
        df: extended master table with new item(s)
    requirements:
        module(s): pandas
    """
    s_my_name = 'master_table_update'
    # we compare them with sets form
    # se_dim = set(df_dim[s_column_label].to_list())
    se_dim = set(df_dim.to_list())
    se_master = set(df_master[s_column_label].to_list())
    # make an additive list
    ls_dim = list(se_dim - se_master)
    # if there is'nt new element(s), return with original dataFrame
    if not ls_dim:
        print(f'Function {s_my_name}: No new element was added to the master table')
        return df_master
    # calc new id list
    ls_master_id = list(range(len(df_master)+1,len(df_master) + len(ls_dim)+1))
    # create a dict with the lists
    di_company_add = {s_id_name: ls_master_id, s_column_label: ls_dim}
    # put a dataFrame
    df_add = pd.DataFrame(di_company_add)
    # concat them
    df_master = pd.concat([df_master, df_add], ignore_index=True)
    print(f'Function {s_my_name}: master table was updated!')
    return df_master

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def replace_label_to_master_id(df_data: pd.DataFrame, df_master: pd.DataFrame, s_join_colname: str) -> pd.DataFrame:
    """ join master table id to df_data and remove original label column
    ver: 1.0
    Params:
        df_data (df): main dataFrame
        df_master (df): master table to join
        s_join_colname (str): join on column name
    Returns:
        df: main dataFrame with new id a removed label columns
    requirements:
        module(s): pandas
    """
    # join master tables data
    df_data = df_data.merge(df_master, on=s_join_colname)
    # drop label coulumns
    df_data.drop([s_join_colname], axis=1, inplace=True)
    return df_data

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
