import os
import pandas as pd
from DataCreation import last_day_in_bases,complate_stock_marcket
import math
import numpy as np
from tqdm import tqdm

main_path = os.path.dirname(os.getcwd())
data_path = main_path + "/data"
close_df_path = data_path + "/Close_df.csv"

#print(last_day_in_bases(close_df_path))
#complate_stock_marcket("2024-02-13", "2024-02-21", main_path, data_path, complete_extraction=False)

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

#print(inputing_data(interval_df))

#close_df = pd.read_csv(data_path + "/Close_df.csv")
#

#proob_close_df = close_df[(close_df.index >= 0) & (close_df.index < 30)]
#proob_clean_df = inputing_data(proob_close_df)

def my_corr(cleaned_df):
    my_datatime = cleaned_df["Datetime"].to_list()[0]
    stock_values = cleaned_df.drop(["Datetime","Day", "Time"], axis = 'columns')
    my_corr = stock_values.corr()
    upper_right_corr = np.triu_indices(len(my_corr), 1)
    
    corr_values = my_corr.values[upper_right_corr]
    corr_col = my_corr.columns[list(upper_right_corr[0])].to_list()                                
    corr_ind = my_corr.columns[list(upper_right_corr[1])].to_list()

    my_lam = lambda x,y: str(x) + " vs "+ str(y)
    my_map = map(my_lam, corr_col, corr_ind)
    my_df = pd.DataFrame(columns  = list(my_map))
    my_df.loc[len(my_df.index)] = corr_values
    my_df["Datetime"] = my_datatime
    return(my_df)


#print(my_corr(proob_clean_df))






def corr_stock_market(market_df, target_path):
    market_df[["Day", "Time"]] = market_df['Datetime'].str.split(" ", n=1, expand=True)
    days = market_df["Day"].unique()
    corr_market_df = pd.DataFrame()

    for i in tqdm(days):
        close_day = market_df[market_df["Day"] == i].reset_index(drop=True)
        number_groups = math.floor(len(close_day)/30)
        if number_groups == 13:
            for j in range(0, number_groups):
                interval_df = close_day[(close_day.index >= j*30) & (close_day.index < (j+1)*30)]
                interval_df_clean = inputing_data(interval_df)
                corr_df = my_corr(interval_df_clean)
                if len(corr_market_df) ==0:
                    corr_market_df = corr_df
                else:
                    corr_market_df.loc[len(corr_market_df.index)] = corr_df.loc[0]
                

        else:
            for j in range(0, number_groups + 1):
                interval_df = close_day[(close_day.index >= j*30) & (close_day.index < (j+1)*30)]
                interval_df_clean = inputing_data(interval_df)
                corr_df = my_corr(interval_df_clean)
                if len(corr_market_df) ==0:
                    corr_market_df = corr_df
                else:
                    corr_market_df.loc[len(corr_market_df.index)] = corr_df.loc[0]
    corr_market_df.to_csv(target_path + "/CorrStockMarket.csv", header=True, index=False)
    print("The file was created successfully")

close_df = pd.read_csv(data_path + "/Close_df.csv")
corr_stock_market(close_df, data_path)
            

  