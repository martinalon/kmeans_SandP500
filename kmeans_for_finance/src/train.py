from functions import DataCreation
from functions import DataClean
import os
from pathlib import Path
from unipath import Path as upath

train_dir =  os.path.dirname(__file__)
train_dir = upath(train_dir)
main_path = train_dir.parent
data_path = main_path + "/data"
db_path = data_path + "/Low_df.csv"

#print(DataCreation.last_day_in_bases(db_path))
#complate_stock_marcket("2024-02-29", "2024-03-4", main_path, data_path, complete_extraction=False)


# i Have errors with the source path
#x = DataClean.cleaning('Close', True)
#x.Parallel_correlation()