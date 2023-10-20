import glob
import os
import csv
import pandas as pd
import numpy as np

import utils.LTROutils as LT
import utils.landvalutils as LAV

#  Importing data from land valuation has the following steps:
# 1. check if there is new data that has been scraped
# 2. If so, integrate it with the old data
# 3. process duplicates
# 4. use standard property_type
# 5. use standard parishes


# 1. Have new properties been added by scraping?
# If so, add them to the latest_landvaluation_data.csv file
# concatenate the two csv files and then remove duplicates

files_from_scraping = glob.glob('./scraping/*.csv') 
# get the file which was last modified
last_scraped_data = max(files_from_scraping, key=os.path.getmtime)


latest_lv_data = "./data/landvaluation/latest_landvaluation_data.csv"

# 2. Import both files and concatenate them
df = pd.concat([pd.read_csv(latest_lv_data, dtype={"assessment_number": str}), 
                pd.read_csv(last_scraped_data, dtype={"assessment_number": str})], 
                ignore_index=True)


# 3. Process Duplicates (also further below)
##### change to lower
df["property_type"] = df["property_type"].str.lower().str.strip()
df["tax_code"] = df["tax_code"].str.lower().str.strip()
df["address_low"] = df["address"].str.lower().str.strip()
df["building_name_low"] = df["building_name"].str.lower().str.strip()

df = df.drop_duplicates()


# 4. Simplify Categories (Standardise property_type)
# Load Dictionary from CSV
with open('./data/property_type_dict.csv') as csv_file:
    reader = csv.reader(csv_file)
    property_type_dict = dict(reader)

# Map new categories using the dictionary    
df2=df.replace({"property_type": property_type_dict})
df2["property_type"].value_counts() # the last one is currently "land"
print("\n", len(df2["property_type"].value_counts()), "property types identified.\n") # there are currently 12 property_type


##### change ARV to numbers
df2.arv = df2.arv.map(lambda x: int(x.replace(',','').replace('$','')) if isinstance(x, str) else x)
# some ARVs may be NaN, so replace them with 0
df2.arv = df2.arv.replace(np.nan, 0, regex=True)

#### drop all empty columns & rows ####
# delete all empty columns & rows
df2 = df2.dropna(axis=1, how='all')
df2 = df2.dropna(axis=0, how='all')

##### Sanity checks
# Check for Duplicates
dfarv = df2.drop_duplicates(subset=['assessment_number'], keep="last")
if df2.shape[0] != dfarv.shape[0]:
    print("WARNING, THERE SEEM TO BE DUPLICATE ASSESSMENT NUMBERS")
    print("processing duplicates with similar building name:")
    df2 = LAV.process_and_merge_duplicates(df2)
    if df2.shape[0] < 40000:
        print("\n Nr of unique assessment numbers: ", df2.shape[0], "[OK]\n")    
    else:
        print("WARNING, ", df2.shape[0], "ARE TOO MANY DUPLICATE ASSESSMENT NUMBERS!!")
    
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

# 5. Simplify Parishes
df2 = LT.simplify_parishes(df2)

# Select columns of interest    
df_for_export = df2[["assessment_number","arv","tax_code","property_type", "address", "grid", "parish", "building_name"]]
# make sure assessment numbers stay as 9 digit strings
df_for_export.loc[:, 'assessment_number'] = df_for_export['assessment_number'].astype(str)
# save to CSV
df_for_export.to_csv("./data/kw-properties.csv", index=False)


