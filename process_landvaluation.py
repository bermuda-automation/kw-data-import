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

files_from_scraping = glob.glob('./scraping/*.csv') 
# get the file which was last modified
last_scraped_data_file = max(files_from_scraping, key=os.path.getmtime)
scraped = pd.read_csv(last_scraped_data_file, dtype={"assessment_number": str})

latest_lv_data_file = "./data/landvaluation/latest_landvaluation_data.csv"
df = pd.read_csv(latest_lv_data_file, dtype={"assessment_number": str})

# 1. Have new properties been added by scraping?
# If so, 
# - let's find which assessment_numbers have modified values
# - let's maintain old assessment_numbers even if they are not in landvaluation anymore
#    (for the record, we want to keep the old values)
# - let's add the new assessment_numbers to the latest_landvaluation_data.csv file

# Ensure 'assessment_number' is the index for both DataFrames
# so we can use vectorized operations
scraped.set_index('assessment_number', inplace=True)
scraped = scraped.sort_index()

df.set_index('assessment_number', inplace=True)
df = df.sort_index()

# Ensure both DataFrames have the same columns
fields_to_compare = ['arv', 'tax_code', 'property_type', 'address', 'grid', 'parish', 'building_name']
scraped = scraped[fields_to_compare]
df = df[fields_to_compare]

# To compare the dataframes, we need to have the same assessment numbers
df_coincide = df.copy(deep=True)
df_coincide = df_coincide[df_coincide.index.isin(scraped.index)]

scraped_coincide = scraped.copy(deep=True)
scraped_coincide = scraped_coincide[scraped_coincide.index.isin(df.index)]

# mask with changed fields using vectorized operations
changed_fields_mask = (scraped_coincide != df_coincide).any(axis=1)
# Update only the changed fields
df.update(scraped_coincide.loc[changed_fields_mask, fields_to_compare])

# reset index for df
df["assessment_number"] = df.index
df.reset_index(drop=True, inplace=True)

# reset index for scraped
scraped["assessment_number"] = scraped.index
scraped.reset_index(drop=True, inplace=True)
            
# 2. add the new assessment numbers
# Find assessment numbers in df that are not in df23
new_assessment_numbers = scraped[~scraped['assessment_number'].isin(df['assessment_number'])]
# Add these new entries to new_df
df = pd.concat([df, new_assessment_numbers], ignore_index=True)
# reset index
df.reset_index(drop=True, inplace=True)

print(f"Added {len(new_assessment_numbers)} new properties to the dataset.")
print(f"The dataset now has {len(df)} properties.")


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
# Remove any assessment number duplicates left (keep the last one)
df = df2[~df2.assessment_number.duplicated(keep='last')]

# 6. Create a column with the property name.
df["property_name"] = df.apply(LAV.create_property_name, axis=1)

# Select columns of interest    
df_for_export = df[["assessment_number","arv","tax_code","property_type", "address", "grid", "parish", "building_name", "property_name"]]
# make sure assessment numbers stay as 9 digit strings
df_for_export.loc[:, 'assessment_number'] = df_for_export['assessment_number'].astype(str)
# save to CSV
print(f"{len(df_for_export)} properties exported to CSV")
df_for_export.to_csv("./data/kw-properties.csv", index=False)


