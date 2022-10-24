# process property skipper data

import configparser
import utils.skipperutils as skipu

import numpy as np

# Get secret URL API
keys = configparser.ConfigParser()
keys.read("./utils/kw_config.txt")
url = keys.get("skipper", "URL")

# download XML and convert to CSV
csv_data = skipu.download_skipper_xml(url)
# csv_data = 'data/skipper/2022-9-18_skipper_properties.csv'
print("\nLast XML downloaded and saved to ./data/skipper/ \n")
# note that we neeed a strategy to clear up old
# CSV files as they will start taking up space (~1.5MB each)

# load data into dataframe
df = skipu.pd.read_csv(csv_data)
# change everything that is empty with np.nan
# then change all of np.nan with python's None
df = df.fillna(np.nan).replace([np.nan], [None])
# clean assessment number column so we have None, 0 or a proper assessment number
df["assessment_number"] = df.assessment_number.map(lambda x: skipu.clean_ass_nr(x))
df["name"] = df.name.map(lambda x: skipu.clean_address(x))

# make sure land and fractional properties are well labeled
df = skipu.identify_land_and_fractional(df)

# make property type uniform
df = skipu.uniform_property_type(df)
print("properties cleaned, property_type identified.\n")

# flag properties with bad price, address, assessment number, country
df = skipu.clean_and_flag_properties(df)
print("FLAGS added to properties with missing data\n")

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
skipper_property = df[["reference", "skipper_id","assessment_number", "name", "parish", "zip", "flag", 
      "longitude", "latitude",
     "property_type",
     "url", "views", "special_headline", "short_description", "long_description",
     'youtube_id', 'vimeo_id', 'rego_embed_id', 'paradym_url',  'virtual_tour_url', 'virtual_tour_img',
     "images",
     'bedrooms', 'bathrooms', 'half_bathrooms', "lotsize", 'sqft']]

listing = df[["reference", "skipper_id","date_added", "date_relisted", "is_rent", "is_sale", 
                "under_contract", "under_offer", "buyer_type",
               "price", "price_from", "daily_rate",
               'agent']]

skipper_property.to_csv("./data/kw-skipper_properties.csv", index=False)
listing.to_csv("./data/kw-listings.csv", index=False)
print("Export Files exported to CSV into ./data/ \n")
