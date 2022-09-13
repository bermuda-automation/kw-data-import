# Imports
import decimal
import math
import dateutil.parser
from operator import itemgetter

import pandas as pd
import numpy as np

# LTRO functions
from utils.LTROutils import get_assessment_number, \
                            identify_fractionals, identify_land_house_condo, \
                            process_duplicates, find_combined_arv, \
                            add_arv_to_ltro, clean_property_type, \
                            nr_of_decimals, smtm, \
                            get_units, convert_to_ha

df = pd.read_excel("./LTRO/LTRO_2018_2022_v1.xlsx", header=None, skiprows=9)

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
        'Vendor\nName and Nationality', 'Purchaser\nName and Nationality', 
      'parish', 'kill', 'parcel_area', 'kill', 'assessment_number_list', 'address', 
     'Law Firm', 'Mode of\nAcquisition', 'acquisition_date', 'Nature of\nInterest', 
      'price']



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

df.reset_index(drop=True, inplace=True)

# a new columns called "property_type"
# is defined. It will contain either
# "fractional", "land" or "0" otherwise
df = identify_fractionals(df)
df = identify_land_house_condo(df)

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

df = process_duplicates(df)
print(to_process, " rows processed for duplicates")

lv = pd.read_csv("./initial_data/kw-properties.csv")
df = add_arv_to_ltro(df,lv)

# improve sales property type data
df = clean_property_type(df, lv)

# keep only properties such that the sales price is more than 3 years of rent.
df = df[~(df['combined_arv']*3 >= df['price'])] #  & (df.property_type != 'fractional')]


# clean_area
unified_area = []
for idx, row in df.iterrows():
    si = str(row.parcel_area).lower()
    
    if si != 'nan':
        contains_digit = any(map(str.isdigit, si))
        contains_units = any
        if contains_digit:
            uni = get_units(si)
            # which one of those units appears first?
            if len(uni)==1:
                first_unit = uni[0]
            
            elif len(uni)>1:
                where_are_they = map(lambda x: si.find(x) , uni)
                wat = list(where_are_they)
                pos_min = min(enumerate(wat), key=itemgetter(1))[0]
                first_unit = uni[pos_min]
            
            else: # no unit in this string
                first_unit = None
                unified_area.append(None)
                continue
            
            # Two properties or a single one?
            one_or_several = si.split(first_unit)
            
            if len(one_or_several) > 2:
                # there are several lands with the same unit
                # could be improved latter by trying to add them up
                unified_area.append(None)
            else:
                converted_surface = convert_to_ha(one_or_several[0], first_unit)
                if converted_surface:
                    unified_area.append(round(converted_surface, 3))
                else:
                    unified_area.append(None)
                
                # just a single land. Save it to standard units
            
        else: # does not contain digit
            unified_area.append(None)
    else: # it is nan
        unified_area.append(None)
        
    # 1. is there a number?
    # 2. is there a unit?
    # if not save zero, otherwise convert to Ha and store

# Create New Dataframe Column
df['parcel_area_ha'] = unified_area

# Save to CSV
final_df = df[["application_number", "registration_date", "parish", "parcel_area", "parcel_area_ha",
              "assessment_number_list", "acquisition_date", "price", "arv", "combined_arv", "property_type"]]
final_df.assessment_number_list = final_df.assessment_number_list.map(get_assessment_number)
final_df.to_csv("./preprocessed_data/kw-sales.csv", index=False)

