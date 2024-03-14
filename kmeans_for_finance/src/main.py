import asyncio
from fastapi import FastAPI
from functions import DataCreation
import os
from pathlib import Path
from unipath import Path as upath


train_dir =  os.path.dirname(__file__)
train_dir = upath(train_dir)
main_path = train_dir.parent
data_path = main_path + "/data"

#print(db_path)
#
#complate_stock_marcket("2024-02-29", "2024-03-4", main_path, data_path, complete_extraction=False)

app = FastAPI()

@app.get("/{feature}")
async def get_name(feature):
    db_path = data_path + "/"+ feature + "_df.csv"
    ld_in_db = DataCreation.last_day_in_bases(db_path)
    return {ld_in_db}

