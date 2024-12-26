
import pandas as pd
import re
from typing import List

def df_fix_str_cols_to_dtime_and_conv(df_data: pd.DataFrame, s_columns: List[str]) -> pd.DataFrame:
    """
    Fix str columns in a DataFrame by adding a time part ('00:00:00') to dates 
    that only have the date part, based on a given regex pattern and convert the columns to datetime fromat
    ver: v1.0
    Args:
        df_data (pd.DataFrame): The DataFrame to process.
        s_columns (List[str]): List of column names to apply the fix
    Returns:
        pd.DataFrame: The updated DataFrame with fixed date columns
    """
    # precompile the regex pattern
    re_only_date = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    # Apply the fix to the specified columns
    for s_column in s_columns:
        df_data[s_column] = df_data[s_column].apply(
            lambda x: f"{x} 00:00:00" if re_only_date.match(str(x)) else x
        )
        # Convert the column to datetime format
        df_data[s_column] = pd.to_datetime(df_data[s_column], errors='coerce')
    return df_data
