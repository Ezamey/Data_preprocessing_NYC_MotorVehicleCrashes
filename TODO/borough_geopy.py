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

