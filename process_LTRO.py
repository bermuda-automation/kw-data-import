# Imports
import decimal
import math
import dateutil.parser


import pandas as pd
import numpy as np

# LTRO functions
import utils.LTROutils as LT


# parameters:
recent_LTRO_data = "./data/LTRO/LTRO_2018_2022.xlsx"
old_processed_LTRO_data = "./data/LTRO/LTRO_2018.csv"

df = pd.read_excel(recent_LTRO_data, header=None, skiprows=9)
older_ltro = pd.read_csv(old_processed_LTRO_data)

# delete all empty columns & rows
df = df.dropna(axis=1, how='all')
df = df.dropna(axis=0, how='all')
    
# merge the two headers which are weirdly split over two rows
# https://stackoverflow.com/q/44799264/
merged_header = df.loc[0].combine_first(df.loc[1])

# turn that into a list
header_list = merged_header.values.tolist()

# hack for 2022 as the sheet has yet again different headers
new_header_list = ['application_number', 'kill', 'sale_type', 'kill', 'registration_date', 
                   'kill', 'kill', 'parish', 'kill', 'parcel_area', 'kill',
                   'assessment_number_list', 'address', 'kill',
                   'Mode of\nAcquisition', 'acquisition_date', 'Nature of\nInterest', 'price']



# load that list as the new headers for the dataframe
# mark for removal with "kill" keyword
# remove the top 2 rows with the old headers
df.columns = new_header_list
df.drop("kill", axis=1, inplace=True) # remove empty columns
df.drop(df.index[:2], inplace=True) # remove old headers

# Identify the ones with price of ZERO
# or with empty or string where the price should be
# coerce will convert the string to NaN
df ['price'] = pd.to_numeric(df['price'], errors='coerce')
# if there were any NaN convert them to zero
df = df.replace(np.nan, 0, regex=True)

# Remove sales for less $1000
# as these are not real sales
# and will shift the average values 
# these are generally government leases for a symbolic price
df = df[df.price >= 1000]


# Remove properties which are unidentifiable:
# - have no assessment number 
# - have assessment number "unknown"
# - have no address: Many just have a short code like SM-800/1, DE-1886/A, *, etc
df = df.drop(df[(df.address.str.len() < 10) & (df.assessment_number_list == 0)].index)
df = df.drop(df[(df.address.str.len() < 10) & (df.assessment_number_list == "Unknown")].index)
# unidentified = df[(df.address.str.len() < 10) & (df.assessment_number_list == 0)] # .price.sum()
# unidentified.to_excel("./data/LTRO/unidentified.xlsx")


# Remove time from the timestamps
df['registration_date'] =  pd.to_datetime(df['registration_date'], format='%Y-%m-%d %H:%M:%S.%f').dt.date
df.reset_index(drop=True, inplace=True)


df = pd.concat([older_ltro,df])
df.reset_index(drop=True, inplace=True)



# a new columns called "property_type"
# is defined. It will contain either
# "fractional", "land" or "0" otherwise
df = LT.identify_fractionals(df)
df = LT.identify_land_house_condo(df)

# Remove Duplicates
LTRO_entries = df.shape[0]
df = df.drop_duplicates(subset=['application_number','registration_date', 
                                'acquisition_date',
                                'assessment_number_list',
                                'price'], 
                                keep='first', )

print(LTRO_entries - df.shape[0], "duplicates removed")

if df[df['application_number'].duplicated()].shape[0] > 0:
    print("WARNING, some sales HAVE DUPLICATES")

# keep=False to show all duplicate entries (not just the ones after the first)
to_process = df[df['application_number'].duplicated(keep=False)].shape[0]

df = LT.process_duplicates(df)
print(to_process, " rows processed for duplicates")

lv = pd.read_csv("./data/kw-properties.csv")
df = LT.add_arv_to_ltro(df,lv)

# improve sales property type data
df = LT.clean_property_type(df, lv)

# keep only properties such that the sales price is more than 3 years of rent.
df = df[~(df['combined_arv']*3 >= df['price'])] #  & (df.property_type != 'fractional')]

df = LT.clean_area(df)


# Save to CSV
final_df = df[["application_number", "registration_date", "parish", "parcel_area", "parcel_area_ha",
              "assessment_number_list", "acquisition_date", "price", "arv", "combined_arv", "property_type"]].copy(deep=False)
final_df.assessment_number_list = final_df.assessment_number_list.apply(LT.get_assessment_number)

final_df.to_csv("./data/kw-sales.csv", index=False)

print("\n >> LTRO DATA IMPORTED << \n")


