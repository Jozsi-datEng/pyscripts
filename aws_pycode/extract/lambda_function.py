# imports
import json
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3

# global variables
s_fdate = datetime.now().date() - relativedelta(months=2)
s_bucket ="jozsi-chicago-taxi-bb"
# s_log_pref = 'ACTION '


#1. get T-2 months taxi data - done
#2. get T-2 months weather data -done
#3. upload to s3 (raw_data/to_processed/weather_data and raw_data/to_processed/taxi_data) - done
#4. create function to originase the code
#5. creating trigger

def get_api_data(url_in='', params_in=None):
    """
    Fetches data from a specified API endpoint and returns the data in JSON format.
    Args:
        url_in (str): The URL of the API endpoint to send the GET request to.
        params_in (dict): A dictionary of parameters to include in the GET request.
    Returns:
        str: The JSON response from the API as a string.
    """
    response = requests.get(url=url_in, params=params_in)
    js_data = response.json()
    return json.dumps(js_data)

def saveto_s3(s_path_in, js_data_in):
    """
    Saves the JSON data to Amazon S3 bucket.
    Args:
        s_path_in (str): The path (key) where the file will be stored in the S3 bucket.
        js_data_in (str): The JSON data to store in the S3 bucket.
    Returns:
        str: A confirmation message indicating the data was saved.
    """
    client = boto3.client("s3")
    client.put_object(
        Bucket=s_bucket,
        Key=s_path_in,
        Body=js_data_in
    )
    return 'data was saved'

def lambda_handler(event, context):
    """
    Main AWS Lambda function handler that fetches taxi and weather data for a given date 
    and saves them to an S3 bucket.
    Args:
        event (dict): The event data passed to the Lambda function
        context (object): The context object provided by AWS Lambda
    Returns:
        None
    """
    # get 1 day taxi data
    url_chportal_taxi_api = (f"https://data.cityofchicago.org/resource/ajtu-isnz.json?"
        f"$where=trip_start_timestamp >= '{s_fdate}T00:00:00' "
        f"AND trip_start_timestamp <= '{s_fdate}T23:59:59'&$limit=30000")
    js_taxi_data = get_api_data(url_chportal_taxi_api)
    saveto_s3(f"raw_data/to_process/taxi_data/taxi_data_{s_fdate}.json", js_taxi_data)

    # get 1 day meteto data
    url_meteo = "https://archive-api.open-meteo.com/v1/era5"
    di_meteo_pars = {
    "latitude": 41.85,
    "longitude": -87.65,
    "start_date": s_fdate,
    "end_date": s_fdate,
    "hourly": "temperature_2m,wind_speed_10m,rain,precipitation"
    }
    js_weather_data = get_api_data(url_meteo, di_meteo_pars)
    saveto_s3(f"raw_data/to_process/weather_data/weater_data_{s_fdate}.json", js_weather_data)
   