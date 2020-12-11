import sys
import pandas as pd
import numpy as np
import time

# from multiprocessing import Pool
from functools import partial
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from geothread import Geopy


df = pd.read_csv("../datasets/data_100000.csv")


# divide the dataframe
chunks = [df[x : x + 1000] for x in range(0, len(df), 1000)]

# ---------------------------Experiment parts(^^)
# Deducing borough with gopy

# Threads
our_threads = []
for i, chunk in enumerate(chunks):
    new_geopy = Geopy("Becode {}".format(i), dataframe=chunk["location"])
    new_geopy.start()
    our_threads.append(new_geopy)

for threads in our_threads:
    threads.join()


def quick_clean(item_series):
    """
    function who  replace the borough name find  by get_borough()
    to keep the original names present in the dataframe
    """
    if "Manha" in item_series:
        return "MANHATTAN"
    if "Brook" in item_series:
        return "BROOKLYN"
    if "Bronx" in item_series:
        return "BRONX"
    if "statan" in item_series:
        return "STATEN ISLAND"
    if "queens" in item_series:
        return "QUEENS"


df["borough_geopy"] = df["location"].apply(quick_clean)
# ---------------------------------------------------------------------------------
# Transform the values of the column "borough" into binaries
print("Transform the values of the column 'borough' into binaries")

test = pd.get_dummies(df["borough_geopy"], prefix="borough_is")
df = pd.concat([df, test], axis=1)

df.drop(["borough", "borough_geopy"], inplace=True, axis=1)
df.fillna("Unspecified", axis=1, inplace=True)

df.to_csv("test_geopy.csv")
