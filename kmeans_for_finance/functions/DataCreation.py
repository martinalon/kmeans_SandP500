import yfinance as yf
from datetime import date,timedelta, datetime
import os
import pandas as pd
from tqdm import tqdm



absolute_path = os.path.abspath(os.path.dirname(__file__))

#print(absolute_path)
###############################
###############################
###############################
def calling_data(name: str, start_date:str, end_date:str, interval:str) -> pd.DataFrame:
    """
    This function gives you the financial information for a specific companie.
    Parameters:
        - name: This is the identificacion as it apears in the stock market.
        - start_date: the first day to take acount the information ("yyyy-mm,dd").  
        - end_date: the last day that will be considered ("yyyy-mm,dd"), it will return the 
                    information until the last data of  "end_date - 1 day".  
        - interval: the interval of time between values (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h,
                    1d, 5d, 1wk, 1mo, 3mo).

    Returns: The function will return a pandas data frame with the following columns.
        - Datetime: Date of time of the value.        
        - Open: the value at the begining of the interval of time.
        - High: the highest value in the interval of time.         
        - Low: the lowest value in the interval of time.       
        - Close: the value at the end of the interval of time.   
        - Adj Close: Adjusted close is the closing price after adjustments for all 
                     applicable splits and dividend distributions in the interval of time.
        - Volume: number of a stocks shares that are traded in the interval of time.

    """
    df = yf.download(name, start=start_date, end = end_date, interval=interval) # this is all the information of the market in the interval of time the interval is open 
    df = df.reset_index()
    return(df)

###################################33
#####################################


def interval_maker(start_date:str, end_date:str):
    """
    This function allows you to obtain the extremes of the time intervals 
    and then use them to recover the information. Note that you can only 
    go back 30 days with 1-minute sampling intervals.

    Parameters:
        - start_date: the first day to take acount the information ("yyyy-mm,dd").  
        - end_date: the last day that will be considered ("yyyy-mm,dd"), it will return the 
                    information until the last data of  "end_date - 1 day".

    Return:
        - interval_beginnings
        - interval_ends  
    """
    start_day_datetype = datetime.strptime(start_date, '%Y-%m-%d')#.date()
    end_day_datetype = datetime.strptime(end_date, '%Y-%m-%d')#.date()

    interval_beginnings = [start_day_datetype]
    interval_ends = []

    for i in range(1,35):
        interval_day = start_day_datetype + timedelta(days = i)
        if interval_day <= end_day_datetype:
            if (i+1)%7 == 0:
                interval_end = interval_day
                new_beginin = interval_end + timedelta(days = 1) 
                interval_ends.append(interval_end)
                interval_beginnings.append(new_beginin)
            else:
                pass
        else :
            interval_end = end_day_datetype + timedelta(days = 1)
            interval_ends.append(interval_end)
            break
    return(interval_beginnings[0:len(interval_ends)], interval_ends)
        

#inicio, final = interval_maker("2024-01-08", "2024-02-13")      
#print(inicio)
#print(final)

##########################
##########################


def complete_data_by_minute(name:str, start_day:str, end_day:str) -> pd.DataFrame:
    """
    This function returns the complete information of value markets by minute 
    of the interval o time provided (les thn 30 days).

    Parameters:
        - name: This is the identificacion as it apears in the stock market.
        - start_day: the first day to take acount the information ("yyyy-mm,dd").  
        - end_day: the last day that will be considered ("yyyy-mm,dd"), it will return the 
                    information until the last data of  "end_date - 1 day".
    
    Returns: The function will return a pandas data frame with the following columns.
        - Datetime: Date of time of the value.        
        - Open: the value at the begining of the interval of time.
        - High: the highest value in the interval of time.         
        - Low: the lowest value in the interval of time.       
        - Close: the value at the end of the interval of time.   
        - Adj Close: Adjusted close is the closing price after adjustments for all 
                     applicable splits and dividend distributions in the interval of time.
        - Volume: number of a stocks shares that are traded in the interval of time.
    """
    inicio, final = interval_maker(start_day, end_day )
    main_df = pd.DataFrame(columns=['Datetime', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    for i in range(0, len(inicio)):
        if i != len(inicio) - 1:
            interval_start = str(inicio[i].date())
            interval_end = str(inicio[i+1].date())
            interval_df = calling_data(name, interval_start, interval_end, "1m")
        else:
            interval_start = str(inicio[i].date())
            interval_end = str(final[i].date()) 
            interval_df = calling_data(name, interval_start, interval_end, "1m")
        main_df = pd.concat([main_df, interval_df], axis=0, ignore_index=True)
    main_df = main_df.rename(columns={
        'Datetime': 'Datetime', 
        'Open': name +' Open', 
        'High': name +' High', 
        'Low': name +' Low', 
        'Close': name +' Close', 
        'Adj Close': name +' Adj Close', 
        'Volume': name +' Volume'
        })
    return(main_df)
#print(complete_data_by_minute("AAPL", "2024-01-15", "2024-02-12"))

##############################################################
##############################################################
##############################################################
##############################################################

def complate_stock_marcket(start_day:str, end_day:str, main_path:str, target_path:str, complete_extraction:bool):
    """ 
    This function allows you to create or update market databases with the information of all companies in the 
    S&P 500 index. It is necessary to verify the last day added to the database to follow a chronological order 
    when updating the databases.

    If the database is created from scratch, then you just have to take into account that the first day to be 
    considered has to be within the first 30 days of the current day.

    Parameters:
        - start_day: the first day to take acount the information ("yyyy-mm,dd").  
        - end_day: the last day that will be considered ("yyyy-mm,dd"), it will return the 
                    information until the last data of  "end_date - 1 day".
        - main_path: this is the main path of the proyect.
        - target_path: this is the path where we want the databases to be stored. 
        - complete_extraction: 
            If True: will create new csv files in the traget file. If there are csv files, will rewrite the 
                    documents
            If False: will uodate the csv files that should already be created in the target path. 

    Returns: The function will save the following 4 csv in the target_path or .
        - Open_df.csv: the value at the begining of each minute for all companies.
        - High: the highest value in each minute for all companies.         
        - Low: the lowest value in each minute for all companies.       
        - Close: the value at the end of  each minute for all companies.


    """
    companies_df = pd.read_csv(main_path + "/data/companies.csv")
    symbols = companies_df["Simbolo"]
    open_df = pd.DataFrame()
    low_df = pd.DataFrame()
    high_df = pd.DataFrame()
    close_df = pd.DataFrame()

    for i in tqdm(symbols):
        if i == "AAPL":
            stock_marcket = complete_data_by_minute(i, start_day, end_day)
            open_df[["Datetime", "AAPL Open"]] = stock_marcket[["Datetime", "AAPL Open"]]
            high_df[["Datetime", "AAPL High"]] = stock_marcket[["Datetime", "AAPL High"]]
            low_df[["Datetime", "AAPL Low"]] = stock_marcket[["Datetime", "AAPL Low"]]
            close_df[["Datetime", "AAPL Close"]] = stock_marcket[["Datetime", "AAPL Close"]]
            
        else:
            stock_marcket = complete_data_by_minute(i, start_day, end_day)
            if len(stock_marcket)> 0:
                open_df = open_df.merge(stock_marcket[["Datetime", i + " Open"]], on="Datetime", how='left')
                high_df = high_df.merge(stock_marcket[["Datetime", i + " High"]], on="Datetime", how='left')
                low_df = low_df.merge(stock_marcket[["Datetime", i + " Low"]], on="Datetime", how='left')
                close_df = close_df.merge(stock_marcket[["Datetime", i + " Close"]], on="Datetime", how='left') 
            else:
                pass
    if complete_extraction == True:
        open_df.to_csv(target_path + "/Open_df.csv", header=True, index=False)
        high_df.to_csv(target_path + "/High_df.csv", header=True, index=False)
        low_df.to_csv(target_path + "/Low_df.csv", header=True, index=False)
        close_df.to_csv(target_path + "/Close_df.csv", header=True, index=False)
        print("The data bases were created successfully")
    else:
        old_open_df = pd.read_csv(target_path + "/Open_df.csv")
        old_high_df = pd.read_csv(target_path + "/High_df.csv")
        old_low_df = pd.read_csv(target_path + "/Low_df.csv")
        old_close_df = pd.read_csv(target_path + "/Close_df.csv")

        old_open_df = pd.concat([old_open_df, open_df], axis=0, ignore_index=True)
        old_high_df = pd.concat([old_high_df, high_df], axis=0, ignore_index=True)
        old_low_df = pd.concat([old_low_df, low_df], axis=0, ignore_index=True)
        old_close_df = pd.concat([old_close_df, close_df], axis=0, ignore_index=True)

        old_open_df.to_csv(target_path + "/Open_df.csv", header=True, index=False)
        old_high_df.to_csv(target_path + "/High_df.csv", header=True, index=False)
        old_low_df.to_csv(target_path + "/Low_df.csv", header=True, index=False)
        old_close_df.to_csv(target_path + "/Close_df.csv", header=True, index=False)
        print("The data bases were updated successfully")
    


#main_path = os.path.dirname(os.getcwd())
#data_path = main_path + "/data"
#complate_stock_marcket("2024-02-13", "2024-02-16", main_path, data_path, complete_extraction=False)


def last_day_in_bases(data_base_path:str) -> str:
    """
    This function helps to know the last day in the data base
    Parameters:
        - data_base_path: the full path to the csv file
    
    Returns: gives you a string with the last day in the csv file
             in the format yyyy-mm-dd.

    """
    df = pd.read_csv(data_base_path)
    last_datatime = df.tail()["Datetime"].to_list()[-1]
    last_day = last_datatime[0:10]
    return(last_day)

#data_base_path = data_path + "/Open_df.csv"
#print(last_day_in_bases(data_base_path))

