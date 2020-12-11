from threading import  Thread,RLock
import pandas as  pd
import time
from functools import partial
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

df = pd.read_csv("data_100000.csv")

#divide the dataframe 
chunks = [df[x:x+1000] for x in range(0,len(df ),10000)]

class Geopy(Thread):

    def __init__(self, user_agent,dataframe):
        Thread.__init__(self)
        self.user_agent = user_agent
        self.dataframe = dataframe
    def run(self):
        geolocator = Nominatim(user_agent=self.user_agent)
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        geocode = partial(geolocator.geocode, language="en")
        reverse = partial(geolocator.reverse, language="en")
        self.geolocator = geolocator
        print("{} init".format(geolocator))
        self.dataframe =  self.dataframe.apply(self.findBorough)

    def findBorough(self,item_series):
        reverse = partial(self.geolocator.reverse, language="en")
        try:
            req = reverse(item_series[1:-1]).address.split(",")[3]
            return req
        except TypeError:
            return item_series

    def getGeolocator(self):
        return  self.geolocator
