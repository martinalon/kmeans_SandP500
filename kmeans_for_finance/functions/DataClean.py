import os
import pandas as pd
#from functions.DataCreation import last_day_in_bases,complate_stock_marcket
import math
import numpy as np
from tqdm import tqdm
import multiprocessing
import csv

#main_path = os.path.dirname(os.getcwd())
#data_path = main_path + "/data"
#close_df_path = data_path + "/Close_df.csv"

#print(last_day_in_bases(close_df_path))
#complate_stock_marcket("2024-02-22", "2024-02-28", main_path, data_path, complete_extraction=False)

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

def inputing_data_2v(df):
    stock_values = df.drop(["Datetime","Day", "Time"], axis = 'columns')
    means = stock_values.mean()
    stock_values.fillna(means, inplace=True)
    return(stock_values) 

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

def my_corr_2v(cleaned_df):
    my_datatime_init = cleaned_df["Datetime"].to_list()[0]
    my_datatime_last = cleaned_df["Datetime"].to_list()[-1]
    stock_values = cleaned_df.drop(["Datetime"], axis = 'columns')
    my_corr = stock_values.corr()
    upper_right_corr = np.triu_indices(len(my_corr), 1)
    
    corr_values = my_corr.values[upper_right_corr]
    corr_col = my_corr.columns[list(upper_right_corr[0])].to_list()                                
    corr_ind = my_corr.columns[list(upper_right_corr[1])].to_list()

    my_lam = lambda x,y: str(x) + " vs "+ str(y)
    my_map = map(my_lam, corr_col, corr_ind)
    my_df = pd.DataFrame(columns  = list(my_map))
    my_df.loc[len(my_df.index)] = corr_values
    my_df["Datetime first"] = my_datatime_init
    my_df["Datetime last"] = my_datatime_last
    return(my_df)


#print(my_corr(proob_clean_df))






def corr_stock_market(market_df, target_path:str, full_corr:bool):
    """
    This function creates a csv file with the correlation of all sets of 30 disjunts elements (rows) of all unique column interseccions. 
    Parameters:
        market_df: This is the information of the stock market in a particular time (Close, High, Open, Low).
            The information has to be ordered by time. 
        
        target_path: This will be the path where the final csv with all corralations wil be stored.
        
        full_corr: This indicates wheather the function will calculate the corraltion since the beginig of the market_df (True), or
            if False, the correlation will be calculated sice the last day in the current CorrStockMarket.csv (if any).  

    Returns: a new csv with the name CorrStockMarket.csv , or updates the current CorrStockMarket.csv with the correlations until 
        the last day in market_df
    """
    # The following line splits the Datatime into two new columns in the dataframe with the info of the stock market 
    market_df[["Day", "Time"]] = market_df['Datetime'].str.split(" ", n=1, expand=True) 
    if full_corr:
        # the following line defines a empty data frame to store the correlations in case of complete correlation
        corr_market_df = pd.DataFrame()
        final_messege = "The file was created sucessfully"
    else:
        # the following line reads the current CorrStockMarket.csv file 
        corr_market_df = pd.read_csv(target_path + "/CorrStockMarket.csv")
        # this line allows the function to know what is the last day in the current CorrStockMarket.csv file
        last_corr_day = corr_market_df.tail(1)['Datetime'].str.split(" ", n=1, expand=True)[0].to_list()[0]
        # The followinf line will take all the information in the market_df that is afther the last day in the CorrStockMarket.csv file  
        market_df =  market_df[market_df["Day"] > last_corr_day]
        final_messege = "The file was updated sucessfully"

    # the following  line gives you all the unique days 
    days = market_df["Day"].unique()
    for i in tqdm(days):
        # this line take the info of the market in the day "i" and resets the index
        close_day = market_df[market_df["Day"] == i].reset_index(drop=True)
        # this lines gives me the number og groups with 30 elements that can be done in one day 
        number_groups = math.floor(len(close_day)/30)
        #tipically, it is posible to create 13 groups.
        if number_groups != 13:
            number_groups = number_groups + 1
        else:
            number_groups = 13
        #this loop take groups of 30 consecutive rows and performs a correlation
        for j in range(0, number_groups):
            # This line take the 30 concecutive elements by the index
            interval_df = close_day[(close_day.index >= j*30) & (close_day.index < (j+1)*30)]
            # if there is missed data, complates it with the mean. Uses a function with a for loop
            interval_df_clean = inputing_data(interval_df)
            # then performs a correlation between columns and make the matrix into a vector with non distinc correlations
            corr_df = my_corr(interval_df_clean)
            # if the correlation df is empty, it necesary to put the first row with this condition
            if len(corr_market_df) ==0:
                corr_market_df = corr_df
            # if the corralation df is not empty, then is time to add the following rows
            else:
                corr_market_df.loc[len(corr_market_df.index)] = corr_df.loc[0]
    corr_market_df.to_csv(target_path + "/CorrStockMarket.csv", header=True, index=False)
    print(final_messege)
            
#close_df = pd.read_csv(data_path + "/Close_df.csv")
#corr_stock_market(close_df, data_path, full_corr = False)

###########################
###########################
############################               

def corr_market_by_minute(market_df, target_path:str, full_corr:bool):
    """
    This function creates a csv file with the correlation of all sets of 30 elements (rows) of all unique column interseccions, 
    with progress of one element each time. 
    Parameters:
        market_df: This is the information of the stock market in a particular time (Close, High, Open, Low).
            The information has to be ordered by time. 
        
        target_path: This will be the path where the final csv with all corralations wil be stored.
        
        full_corr: This indicates wheather the function will calculate the corraltion since the beginig of the market_df (True), or
            if False, the correlation will be calculated sice the last day in the current CorrStockMarket.csv (if any).  

    Returns: a new csv with the name CorrStockMarket_by_minute.csv , or updates the current CorrStockMarket.csv with the correlations until 
        the last day in market_df
    """
    # The following line splits the Datatime into two new columns in the dataframe with the info of the stock market 
    market_df[["Day", "Time"]] = market_df['Datetime'].str.split(" ", n=1, expand=True) 
    if full_corr:
        # the following line defines a empty data frame to store the correlations in case of complete correlation
        corr_market_df = pd.DataFrame()
        final_messege = "The file was created sucessfully"
    else:
        # the following line reads the current CorrStockMarket.csv file 
        corr_market_df = pd.read_csv(target_path + "/CorrStockMarket_by_minute.csv")
        # this line allows the function to know what is the last day in the current CorrStockMarket.csv file
        last_corr_day = corr_market_df.tail(1)['Datetime'].str.split(" ", n=1, expand=True)[0].to_list()[0]
        # The followinf line will take all the information in the market_df that is afther the last day in the CorrStockMarket.csv file  
        market_df =  market_df[market_df["Day"] > last_corr_day]
        final_messege = "The file was updated sucessfully"

    # the following  line gives you all the unique days 
    days = market_df["Day"].unique()
    for i in tqdm(days):
        # this line take the info of the market in the day "i" and resets the index
        close_day = market_df[market_df["Day"] == i].reset_index(drop=True)
        for j in tqdm(range(0,len(close_day)- 29)):
            # This line take the 30 concecutive elements by the index
            interval_df = close_day[(close_day.index >= j) & (close_day.index < (j+30))]
            interval_df_clean = inputing_data_2v(interval_df)
            interval_df_clean["Datetime"] = interval_df["Datetime"]
            # then performs a correlation between columns and make the matrix into a vector with non distinc correlations
            corr_df = my_corr_2v(interval_df_clean)
            # if the correlation df is empty, it necesary to put the first row with this condition
            if len(corr_market_df) ==0:
                corr_market_df = corr_df
            # if the corralation df is not empty, then is time to add the following rows
            else:
                corr_market_df.loc[len(corr_market_df.index)] = corr_df.loc[0]
    
    corr_market_df.to_csv(target_path + "/CorrStockMarket_by_minute.csv", header=True, index=False)
    print(final_messege)  
            
             
#close_df = pd.read_csv(data_path + "/Close_df.csv")
#corr_market_by_minute(close_df, data_path, full_corr = True)



def corr_by_minute(day,market_df, target_path, full_corr):

    # this line take the info of the market in the day "i" and resets the index
    close_day = market_df[market_df["Day"] == day].reset_index(drop=True)
    for j in tqdm(range(0, len(close_day)- 29)):
        # This line take the 30 concecutive elements by the index
        interval_df = close_day[(close_day.index >= j) & (close_day.index < (j+30))]
        interval_df_clean = inputing_data_2v(interval_df)
        interval_df_clean["Datetime"] = interval_df["Datetime"]
        # then performs a correlation between columns and make the matrix into a vector with non distinc correlations
        corr_df = my_corr_2v(interval_df_clean)
        
        if full_corr and j == 0:
            columns = (corr_df.columns)
            with open(target_path+ '/' + day +'.csv', 'w') as f:
                writer = csv.writer(f)
                # write the header
                writer.writerow(columns)
        with open(target_path+ '/' + day +'.csv', 'a') as f:
            writer = csv.writer(f)
            # write the header
            writer.writerow(corr_df.loc[0].to_list())
            

#market_df = pd.read_csv(data_path + "/Close_df.csv")
#market_df[["Day", "Time"]] = market_df['Datetime'].str.split(" ", n=1, expand=True) 
#days = market_df["Day"].unique()
#corr_by_minute(days[0])

class cleaning:
    def __init__(self, feature, full_corr):
        self.feature = feature
        self.full_corr = full_corr
    
    
    def Parameters_Pcorr(self, day):
        main_path = os.path.dirname(os.getcwd())
        data_path = main_path + "/data"
        coorelations_path = data_path + "/Correlations_by_day"
        market_path = data_path + "/" + self.feature + "_df.csv"
        market_df = pd.read_csv(market_path)
        market_df[["Day", "Time"]] = market_df['Datetime'].str.split(" ", n=1, expand=True) 
        corr_by_minute(day, market_df = market_df, target_path = coorelations_path, full_corr = self.full_corr)
         
    def Parallel_correlation(self):
        main_path = os.path.dirname(os.getcwd())
        data_path = main_path + "/data"
        market_path = data_path + "/" + self.feature + "_df.csv"
        market_df = pd.read_csv(market_path)
        market_df[["Day", "Time"]] = market_df['Datetime'].str.split(" ", n=1, expand=True)
        days = market_df["Day"].unique()
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        result = pool.map(self.Parameters_Pcorr, days)
        print("The documents have been created or actualized")

#x = cleaning('Close', True)
#x.Parallel_correlation()
#print(x.full_corr)


