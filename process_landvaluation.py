import glob
import os
import csv
import pandas as pd

import utils.LTROutils as LT
import utils.landvalutils as LAV

# Have new properties been added by scraping?
# If so, add them to the latest_landvaluation_data.csv file
# concatenate the two csv files and then remove duplicates

files_from_scraping = glob.glob('./scraping/*.csv') 
last_scraped_data = max(files_from_scraping, key=os.path.getctime)


latest_lv_data = "./data/landvaluation/latest_landvaluation_data.csv"

# Import both files and concatenate them
df = pd.concat([pd.read_csv(latest_lv_data), pd.read_csv(last_scraped_data)], ignore_index=True)


df = df.drop_duplicates()
##### change to lower
df["property_type"] = df["property_type"].str.lower().str.strip()
df["tax_code"] = df["tax_code"].str.lower().str.strip()
df["address_low"] = df["address"].str.lower().str.strip()
df["building_name_low"] = df["building_name"].str.lower().str.strip()

##### Simplify Categories
# Load Dictionary from CSV
with open('./data/property_type_dict.csv') as csv_file:
    reader = csv.reader(csv_file)
    property_type_dict = dict(reader)

# Map new categories using the dictionary    
df2=df.replace({"property_type": property_type_dict})
df2["property_type"].value_counts() # the last one is currently "land"
len(df2["property_type"].value_counts()) # there are currently 19 property_type


##### change ARV to numbers
df2.arv = df2.arv.map(lambda x: int(x.replace(',','').replace('$','')))


##### Sanity checks
# Check for Duplicates
dfarv = df2.drop_duplicates(subset=['assn_nr'], keep=False)
if df2.shape[0] != dfarv.shape[0]:
    print("WARNING, THERE SEEM TO BE DUPLICATE ASSESSMENT NUMBERS")
    print("processing duplicates with similar building name:")
    df2 = LAV.process_and_merge_duplicates(df2)
    print("\n Nr of unique assessment numbers: ", df2.shape[0], "[OK]\n")    
    
else:
    print("# of unique assessment numbers: ", df2.shape[0], "[OK]")    
# Inspect ARV range
mxarv = df2["arv"].max()
minarv = df2["arv"].min()
if minarv < 0:
    print("NEGARIVE ARVs, please inspect data")
if mxarv > 10000000:
    print("ARVs too large, please inspect data")
else:
    print("Min ARV:", minarv, "and", "Max ARV", mxarv,  "[OK]")

df2 = LT.simplify_parishes(df2)
    
df_for_export = df2[["assn_nr","arv","tax_code","property_type", "address", "grid", "parish", "building_name"]]
# save to CSV
df_for_export.to_csv("./data/kw-properties.csv", index=False)


