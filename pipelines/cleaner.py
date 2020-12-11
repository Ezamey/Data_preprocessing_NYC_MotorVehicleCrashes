#imports 
import pandas as pd
import numpy as np
import  time

from  main import NY_dataset,export_name
from pipelines.functions import drop_too_much,searching_possible_transformable_variables,is_inDay

print("Starting cleaner.py")
#working on a copy
NY_copy = NY_dataset.copy()

#Original dataset in in CAPITAL CASES :
NY_copy.columns= NY_copy.columns.str.lower()
#And without "_"
NY_copy.columns= NY_copy.columns.str.replace(" ","_")

#Quick dealing with missing  zip
try:
    NY_copy.sort_values(by="location").zip_code.fillna(method="ffill")
except AttributeError:
    print("Attribute Error :-> pass")

#removing col with more then 70% missing values
print("Removing col with more than 70% missing values")
cols = drop_too_much(NY_copy,val=0.7)
NY_copy.drop(cols,axis=1,inplace=True)

#Convert date to datetime 
print("Convert date to datetime ")
NY_copy["crash_date"] = pd.to_datetime(NY_copy["crash_date"],infer_datetime_format=True)

#Create a column InDay with 0/1 value based of the datetime of the accident
print("Create a column InDay with 0/1 value based of the datetime of the accident")
NY_copy["is_inDay"]= NY_copy["crash_time"].apply(is_inDay)

#We still have a lot of missing values. 
# We note that zip/code and borough have almost the same but not latitude and longitude.
# So maybe we can fill some with their coordonates and fill some lat/long with street name.
# see borough_finder.py

# if there is absurd values and  "streat_name" isnull()>true,  we can conclude that those data are not valuables
to_drop = NY_copy[(NY_copy["latitude"]<=1) & (NY_copy["longitude"]>=-1) | (NY_copy["longitude"]<=77) & (NY_copy["on_street_name"].isnull())].index
NY_copy.drop(to_drop,inplace=True)


#Tthe values in columns "vehicle_type_code"1 and 2 can be consolidated
#The goal is to reunite some type into subtype.
print("Reuniting vehicles into classes")
try:
    vehicles = [*map(str.lower,list(NY_copy["vehicle_type_code1"].value_counts().index))]
except KeyError:
    vehicles = [*map(str.lower,list(NY_copy["vehicle_type_code_1"].value_counts().index))]

deliv_type = [ truck for truck in vehicles if "deli" in truck]

medical_type = []
for infos in vehicles:
    if "ambul" in infos or "ambu" in infos:
        medical_type.append(infos)

car_type = []
for infos in vehicles:
    if "car" in infos or "sedan" in infos or "cab" in infos or "chevr" in infos:
        car_type.append(infos)

two_weels_type = []
for infos in vehicles:
    if "scoot" in infos or "cycle" in infos or "mot" in infos:
        two_weels_type.append(infos)

truck_type = [ truck for truck in vehicles if "truck" in truck]
for infos in vehicles:
    if "util" in infos or "wagon" in infos:
        truck_type.append(infos)

def vehicle_type(item):
    item = str(item).lower()
    if item in medical_type:
        return "Medical"
    if item in car_type:
        return "Car"
    if item in two_weels_type:
        return "MotoCycle"
    if item in deliv_type:
        return "Delivery Vehicle"
    if item in truck_type:
        return "Truck"
    else:
        return "Others Category"

try:
    NY_copy["vehicle_type_code1"] = NY_copy["vehicle_type_code1"].apply(vehicle_type)
    NY_copy["vehicle_type_code2"] = NY_copy["vehicle_type_code2"].apply(vehicle_type)
except KeyError:
    NY_copy["vehicle_type_code1"] = NY_copy["vehicle_type_code_1"].apply(vehicle_type)
    NY_copy["vehicle_type_code2"] = NY_copy["vehicle_type_code_2"].apply(vehicle_type)

# Now that we have done that, we can  transform those columns into binaries values
print("Transforming those columns into binaries values")
test = pd.get_dummies(NY_copy["vehicle_type_code1"],prefix="vehicle1_type_is")
NY_copy = pd.concat([NY_copy,test],axis=1)
test = pd.get_dummies(NY_copy["vehicle_type_code2"],prefix="vehicle2_type_is")
NY_copy = pd.concat([NY_copy,test],axis=1)

# FINAL :
print("First_save")
final = NY_copy.copy()

final.drop(["vehicle_type_code1","vehicle_type_code2","collision_id"],inplace=True,axis=1)

final.to_csv(export_name)



