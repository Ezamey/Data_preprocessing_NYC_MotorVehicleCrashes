from threading import Thread, RLock
import pandas as pd
from datetime import datetime
from functools import partial
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#df = pd.read_csv("./datasets/data_100000.csv")
df = pd.read_csv("D:/BECODE/Git/Data_preprocessing_NYC_MotorVehicleCrashes/datasets/data_100000.csv")
# divide the dataframe
test = df[:100]


class Geopy(Thread):
    def __init__(self, user_agent, dataframe):
        Thread.__init__(self)
        pd.options.mode.chained_assignment = None  # default='warn'
        self.user_agent = user_agent
        self.dataframe = dataframe
        self.geolocator = Nominatim(user_agent=self.user_agent)

    def run(self):
        
        geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)
        geocode = partial(self.geolocator.geocode, language="en")
        reverse = partial(self.geolocator.reverse, language="en")
        print("{} init".format(self.geolocator))
        self.dataframe["geopy"] = self.dataframe["location"].apply(self.findBorough)

    def findBorough(self, item_series):
        reverse = partial(self.geolocator.reverse, language="en",timeout=10000)
        try:
            req = reverse(item_series[1:-1]).address.split(",")[3]
            return req
        except TypeError:
            return item_series
    
    def quick_clean(self,item_series):
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

    def getDataFrame(self):
        return self.dataframe

if __name__ == "__main__":

    #With threads
    startTime = datetime.now()
    print(startTime)
    numero_1 = Geopy("BEcode_1",test[:20])
    numero_2 = Geopy("BEcode_2",test[20:40])
    numero_3 = Geopy("BEcode_3",test[40:])
    numero_1.start()
    numero_2.start()
    numero_3.start()

    numero_1.join()
    numero_2.join()
    numero_3.join()
    trds_frame =  [numero_2.getDataFrame(),numero_1.getDataFrame(),numero_3.getDataFrame()]

    final = pd.DataFrame()
    for trds in  trds_frame:
        final = pd.concat([final,trds])
    print(final.head())
    print(datetime.now() - startTime)


    #Without threads
    startTime = datetime.now()
    print(startTime)
    numero_1 = Geopy("BEcode_1",test)
    numero_1.start()
    numero_1.join()

    final = numero_1.getDataFrame()
    print(final.head())
    print(datetime.now() - startTime)

    
