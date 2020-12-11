import sys
import pandas as pd
import numpy as np
import time


if len(sys.argv) == 1:
    # importing sets
    path = "../final.csv"
    NY_dataset = pd.read_csv(path)
    export_name = "final_with_borough.csv"

if len(sys.argv) > 1:
    path = sys.argv[1]
    chunksize = 100000
    chunks = []
    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
        chunks.append(chunk)
    NY_dataset = pd.concat(chunks, axis=0)
    export_name = sys.argv[1] + "with_borough.csv"

# ---------------------------Experiment parts(^^)
# Deducing borough with long
print("Deducing borough using longitude")
count = 0


def deduce_borough(item_series):
    bor = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]
    global count
    count += 1
    if item_series not in bor:
        try:
            return long_inter(count)
        except IndexError:
            return item_series
    else:
        return item_series


def long_inter(index):
    x = NY_dataset.iloc[count][["longitude", "latitude"]]
    if (
        x["longitude"] > -73.93161 and x["longitude"] < -73.786210
    ):  # and (x["latitude"]>40.798573 and x["latitude"]<-40.910320):
        return "BRONX"
    if x["longitude"] > -74.04072 and x["longitude"] < -73.857280:
        return "BROOKLYN"
    if x["longitude"] > -74.01794 and x["longitude"] < -73.912160:
        return "MANHATTAN"
    if x["longitude"] > -73.95933 and x["longitude"] < -73.700584:
        return "QUEENS"
    if x["longitude"] > -74.24857 and x["longitude"] < -74.061000:
        return "STATEN ISLAND"
    return "Not declared"


NY_dataset["borough_deducing"] = NY_dataset["borough"].apply(deduce_borough)

# its obviously flawed since only longitude are used. Need to add latitude booleans.
# Using geopy takes to much time for the execice.
# ---------------------------------------------------------------------------------

# Transform the values of the column "borough" into binaries
print("Transform the values of the column 'borough' into binaries")

test = pd.get_dummies(NY_dataset["borough_deducing"], prefix="borough_is")
df = pd.concat([NY_dataset, test], axis=1)

df.drop(["borough_deducing"], inplace=True, axis=1)
df.fillna("Unspecified", axis=1, inplace=True)

df.to_csv("../" + export_name)
