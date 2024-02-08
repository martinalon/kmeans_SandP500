import yfinance as yf
from datetime import date,timedelta, datetime
import os
import pandas as pd

#today = date.today()
#yesterday = today - timedelta(days = 1)

#print(today)
#print(yesterday)
# days for market indices
#start_date = "2022-01-01"
#end_date = yesterday #this is only to prove the model
#end_date = today

#days for crypto currencies 
#cripto_start_date = "01-01-2022"
#cripto_end_date = str(yesterday.day)+ "-" + str(yesterday.month) + "-" +  str(yesterday.year)

absolute_path = os.path.abspath(os.path.dirname(__file__))

#df = yf.download("AAPL", period = 'max', interval="1m") # this is all the information of the market in the interval of time the interval is open 
#df = df.reset_index()
#print(df.head())
#print(df.tail())
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
    Esta funcion permite obtener los extremos de los intervalos de tiempo para despues
    utilizarlos para recuperar la informacion, Notemos que solo se puede ir 30 dias atras
    con intervalos de 1 minuto de muestreo.

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

    for i in range(1,30):
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
    return(interval_beginnings, interval_ends)
        

inicio, final = interval_maker("2024-01-08", "2024-02-13")      

print(inicio)
print(final)

   





