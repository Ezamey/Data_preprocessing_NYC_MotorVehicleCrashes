import sys
import pandas as pd
from pipelines import cleaner


if len(sys.argv) == 1:
    # importing sets
    path = "datasets/data_100000.csv"
    NY_dataset = pd.read_csv(path)
    export_name = "final.csv"

if len(sys.argv) > 1:
    path = (
        "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"
    )
    chunksize = 100000
    chunks = []
    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
        chunks.append(chunk)
    NY_dataset = pd.concat(chunks, axis=0)
    export_name = sys.argv[1] + ".csv"
