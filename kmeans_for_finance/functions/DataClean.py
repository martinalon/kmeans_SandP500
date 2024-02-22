import os
import pandas as pd
from DataCreation import last_day_in_bases,complate_stock_marcket

main_path = os.path.dirname(os.getcwd())
data_path = main_path + "/data"
close_df_path = data_path + "/Close_df.csv"

#print(last_day_in_bases(close_df_path))
#complate_stock_marcket("2024-02-13", "2024-02-21", main_path, data_path, complete_extraction=False)
close_df = pd.read_csv(data_path + "/Close_df.csv")
interval_df = close_df[(close_df.index >= 0 ) & (close_df.index <30)]
#################################
#################################
#################################
def inputing_data(df):
    columns = df.columns
    for i in columns:
        boleans = df[i].isnull().any()
        if boleans:
            df[i] = df[i].fillna(df[i].mean())
        else:
            pass
    return(df) 

#print(inputing_data(interval_df).head())