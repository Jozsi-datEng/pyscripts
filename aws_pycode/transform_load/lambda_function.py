import pandas as pd
from myfunc import *

s_mybucket = "jozsi-chicago-taxi-bb"
pathdir_proc_taxi = 'raw_data/to_process/taxi_data/'
pathdir_processed_taxi = 'raw_data/processed/taxi_data/'
pathdir_transf_taxi = 'transfomed_data/taxi_data/'
pathdir_proc_wheater = 'raw_data/to_process/weather_data/'
pathdir_processed_wheater = 'raw_data/processed/weather_data/'
pathdir_transf_wheater = 'transfomed_data/weather_data/'
pathdir_pay_type_master = 'transfomed_data/payment_type/'
pathdir_company_master = 'transfomed_data/company/'
pathdir_prev_masters = 'transfomed_data/master_table_previous_version/'
s_pay_types = 'payment_types.csv'
s_companies = 'companies.csv'
s_taxi_data = 'taxi_data.csv'
s_wheater_data = 'wheater_data.csv'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def lambda_handler(event, context):
    # # # main program control section  # # #
    
    # 1 - load and process wheater_data
    # load weathe data to df from s3 csv
    dic_files = load_s3_dir_files(
        s_bucket=s_mybucket, 
        path_work = pathdir_proc_wheater, 
        s_file_sufix='json'
        )
    # iteration on the received dict
    if not dic_files.keys(): print(f'There was no ".json" file in {pathdir_proc_wheater} to process!')
    for s_fname in dic_files.keys():
        # 2.1 - clean & transform data
        df_wheater_data = trasform_daily_wheater_data(dic_files[s_fname])

        # 1.2 - upload results to s3
        s_fdate = str(df_wheater_data['datetime'].iloc[0].date()) #strftime("%Y-%m-%d")
        path_file = pathdir_transf_wheater + s_wheater_data.replace('.csv', '_'+s_fdate+'.csv') # insert date to filename
        save_df_to_s3(bucket=s_mybucket, path_file=path_file, df_data=df_wheater_data)
        path_file = pathdir_proc_wheater + s_fname
        path_file_new = pathdir_processed_wheater + s_fname
        move_file_on_s3(bucket=s_mybucket, path_file=path_file,path_file_new=path_file_new)

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
   
    # 2 - load and process taxi data
    # load taxi data to df from s3 csv
    dic_files = load_s3_dir_files(
        s_bucket=s_mybucket, 
        path_work = pathdir_proc_taxi, 
        s_file_sufix='json'
        )
    # iteration on the received dict
    if not dic_files.keys(): print(f'There was no ".json" file in {pathdir_proc_taxi} to process!')
    for s_fname in dic_files.keys():
        df_taxi_data = pd.DataFrame(dic_files[s_fname])
        # 2.1 - clean & transform data
        df_taxi_data = trasform_taxi_data(df_taxi_data)
        # create new dim tables from taxi data
        df_payment_types_new = df_taxi_data['payment_type'].drop_duplicates().reset_index(drop=True)
        df_companies_new = df_taxi_data['company'].drop_duplicates().reset_index(drop=True)
        # load master tables from s3
        df_payment_types = load_s3_csv_to_df(
            s_bucket=s_mybucket, 
            path_file = pathdir_pay_type_master + s_pay_types, 
            )
        df_companies = load_s3_csv_to_df(
            s_bucket=s_mybucket, 
            path_file = pathdir_company_master + s_companies, 
            )
        # update master tables as needed
        df_payment_types = master_table_update(df_master=df_payment_types, df_dim=df_payment_types_new, s_id_name='payment_type_id', s_column_label='payment_type')
        df_companies = master_table_update(df_master=df_companies, df_dim=df_companies_new, s_id_name='company_id', s_column_label='company')
        # join master tables ids to taxi data and remove it's original label columns
        df_taxi_data = replace_label_to_master_id(df_data=df_taxi_data, df_master=df_payment_types, s_join_colname='payment_type')
        df_taxi_data = replace_label_to_master_id(df_data=df_taxi_data, df_master=df_companies, s_join_colname='company')
        # 2.2 - upload results to s3
        # move current master tables to s3 "previous" folder and upload the current file
        path_file = pathdir_pay_type_master + s_pay_types
        path_file_new = pathdir_prev_masters + s_pay_types.replace('.csv', '_prev.csv')
        move_file_on_s3(bucket=s_mybucket, path_file=path_file, path_file_new=path_file_new)
        save_df_to_s3(bucket=s_mybucket, path_file=path_file, df_data=df_payment_types)
        #
        path_file = pathdir_company_master + s_companies
        path_file_new = pathdir_prev_masters + s_companies.replace('.csv', '_prev.csv')
        move_file_on_s3(bucket=s_mybucket, path_file=path_file,path_file_new=path_file_new)
        save_df_to_s3(bucket=s_mybucket, path_file=path_file, df_data=df_companies)
        # move taxi_data.json to s3 "processed" folder and upload the taxi_data.csv file to it's transform tree
        s_fdate = str(df_taxi_data['datetime_for_weather'].iloc[0].date()) #strftime("%Y-%m-%d")
        path_file = pathdir_transf_taxi + s_taxi_data.replace('.csv', '_'+s_fdate+'.csv') # insert date to filename
        save_df_to_s3(bucket=s_mybucket, path_file=path_file, df_data=df_taxi_data)
        path_file = pathdir_proc_taxi + s_fname
        path_file_new = pathdir_processed_taxi + s_fname
        move_file_on_s3(bucket=s_mybucket, path_file=path_file,path_file_new=path_file_new)

    return True
