import os
import pandas as pd
from DataCreation import last_day_in_bases

main_path = os.path.dirname(os.getcwd())
data_path = main_path + "/data"

close_df = pd.read_csv(data_path + "/Close_df.csv")
print(close_df)
