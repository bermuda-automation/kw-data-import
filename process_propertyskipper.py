"""Module to process property skipper data"""
import os
import configparser
from datetime import datetime

import pandas as pd
import numpy as np

import utils.skipperutils as skipu
import utils.skipperstatsutils as SSU
import utils.LTROutils as LT

# Get secret URL API
keys = configparser.ConfigParser()
keys.read("./utils/kw_config.txt")
url = keys.get("skipper", "URL")

# define file to save to
today = datetime.today()
skipper_properties_xml = 'data/skipper/{}-{:02d}-{:02d}_skipper_properties.xml'.format(today.year, today.month, today.day)
# if file exists for today, don't download again
if not os.path.exists(skipper_properties_xml):
    # Get data from web as XML
    SSU.get_xml_with_wget(url, skipper_properties_xml)

print(skipper_properties_xml)
skipper_properties_csv = "data/skipper/{}-{:02d}-{:02d}_skipper_properties.csv".format(today.year, today.month, today.day)
# Open XML and convert to CSV
csv_data = skipu.download_skipper_xml(skipper_properties_xml,
                                      skipper_properties_csv)
# csv_data = 'data/skipper/2023-08-14_skipper_properties.csv'
print("\nLast XML downloaded and saved to ./data/skipper/ \n")

# load data into dataframe
df = pd.read_csv(csv_data)

# change everything that is empty with np.nan
# delete all empty columns & rows
df = df.dropna(axis=1, how='all')
df = df.dropna(axis=0, how='all')

# replace nan with zero
df = df.replace(np.nan, 0, regex=True)

# Make sure prices are numeric
df ['price'] = pd.to_numeric(df['price'], errors='coerce')
# clean assessment number column so we have
#  either a proper assessment number or 0
df["assessment_number"] = df.assessment_number.apply(skipu.clean_assn_nr)
# if address is empty or just a number leave it as zero.
df["name"] = df.name.apply(skipu.clean_address)

# make sure land and fractional properties are well labeled
df = skipu.identify_fractionals(df)
# identify lands and add 'land' in the property_type column
df = LT.identify_lands(df, skipper_dataframe=True)

# make property type uniform
df = skipu.uniform_property_type(df)
print("properties cleaned, property_type identified.\n")

# Use landvaluation to clean up potentially spurious property_type-s
lv = pd.read_csv("./data/kw-properties.csv", dtype={"assessment_number": str})
df = LT.clean_property_type(df, lv)

# flag properties with bad price, address, assessment number, country
df = skipu.clean_and_flag_properties(df)
print(" >>> FLAGS added to properties with missing data\n")

# add property name to skipper properties
df = skipu.add_property_name_to_skipper_properties(df, lv)

# remove carriage returns which give problems when converted to CSV
# \r -> mapped to \n
df = skipu.sanitize_text(df)

# Convert agents column from list to dict:
# https://github.com/bermuda-automation/kw-data-import/issues/3
df["agent"] = df.agent.apply(skipu.clean_up_agent_list).apply(skipu.agent_list_to_dict)

# rename column city -> parish
# make naming uniform
# merge city hamilton -> pembroke and Town of St.George -> St. George
df = skipu.simplify_parishes(df)

# Save to the two CSVs
skipper_property = df[["reference", "skipper_id","assessment_number",
                       "name", "parish", "zip", "flag",
                        "longitude", "latitude", "property_type",
     "url", "views", "special_headline", "short_description", "long_description",
     'youtube_id', 'vimeo_id', 'paradym_url',  'virtual_tour_url', "images",
     # 'virtual_tour_img', 'rego_embed_id' seem to be empty
     'bedrooms', 'bathrooms', 'half_bathrooms', "lotsize", 'sqft', "property_name"]]

listing = df[["reference", "skipper_id","date_added", "date_relisted",
              "is_rent", "is_sale", "under_contract", "under_offer", "buyer_type",
               "price", "price_from", "daily_rate", 'agent', "property_name"]]

skipper_property.to_csv("./data/kw-skipper_properties.csv", index=False, na_rep='')
listing.to_csv("./data/kw-listings.csv", index=False)
print("kw-skipper_properties.csv and kw-listings.csv exported to CSV into ./data/ \n")
