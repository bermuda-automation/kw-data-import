# Imports
import pandas as pd
import numpy as np

# LTRO functions
import utils.LTROutils as LT
import utils.skipperutils as skipu
from utils.LTROutils import NORWOOD_DATA_PATH

#  Importing data from LTRO has the following steps:
# 1. Define files received from LTRO
# 2. Clean files (clean & rename columns, delete empty rows, remove anomalous sales)
# 3. use standard property_type
# 4. process duplicates
# 5. use standard parishes

# 1. Files from LTRO:
DATA_18_22 = "./data/LTRO/LTRO_2018_2022.xlsx"
DATA_18_PROCESSED = "./data/LTRO/LTRO_2018.csv"
DATA_22 = "./data/LTRO/LTRO_2022.xlsx"

df = pd.read_excel(DATA_18_22, header=None, skiprows=9)
older_ltro = pd.read_csv(DATA_18_PROCESSED)
dflast =  pd.read_excel(DATA_22, header=None, skiprows=9)

# 2. Clean Files
df = LT.clean_ltro_data(df)
dflast = LT.clean_ltro_data(dflast)

# combine all 3 files
df = pd.concat([older_ltro, df, dflast])
# if there were any NaN convert them to zero
df = df.replace(np.nan, 0, regex=True)
df['registration_date'] =  pd.to_datetime(df['registration_date'], format='%Y-%m-%d').dt.date
df.reset_index(drop=True, inplace=True)

print(f"\n{df.shape[0]} sales imported between dates:"
      f"{df.registration_date.min()} and "
      f"{df.registration_date.max()}\n")

# 3. a new columns called "property_type"
# is defined. It will contain either
# "fractional", "land", "house", "condo" or False
df = LT.identify_fractionals(df)
df = LT.identify_lands(df)
df = LT.identify_houses(df)
df = LT.identify_condos(df)

# 4. Remove Duplicates
LTRO_entries = df.shape[0]
df = df.drop_duplicates(subset=['application_number','registration_date',
                                'acquisition_date',
                                'assessment_number',
                                'price'], 
                                keep='first', )

print(LTRO_entries - df.shape[0], "duplicates removed")

if df[df['application_number'].duplicated()].shape[0] > 0:
    print("WARNING, some sales HAVE DUPLICATES")

# keep=False to show all duplicate entries (not just the ones after the first)
to_process = df[df['application_number'].duplicated(keep=False)].shape[0]

df = LT.process_duplicates(df)
print(to_process, " rows processed for duplicates")

# Prepare for next phase by cleaning up assessment numbers
df = df.replace(np.nan, 0, regex=True)

df["assessment_number"] = df.assessment_number.apply(skipu.clean_assn_nr)
lv = pd.read_csv("./data/kw-properties.csv", dtype={"assessment_number": str})
df = LT.add_arv_to_ltro(df, lv)


# improve sales property type data
df = LT.clean_property_type(df, lv)


# Sanity check:
# keep only properties such that the sales price is more than 2 years of rent.
df = df[~(df['combined_arv']*2 >= df['price'])] #  & (df.property_type != 'fractional')]

# create a column with surface area in hectares
df = LT.clean_area(df)

# 5. Use standard parishes
df = LT.simplify_parishes(df)

# Select columns of interest
final_df = df[["application_number", "registration_date", "parish",
               "address", "parcel_area", "parcel_area_ha",
              "assessment_number", "acquisition_date",
               "price", "arv", "combined_arv", "property_type"]].copy(deep=False)

# final_df.assessment_number = final_df.assessment_number.apply(LT.get_assessment_number)
final_df["assessment_number"] = final_df.assessment_number.apply(skipu.clean_assn_nr)

# try to find the assessment number or address
# for properties identified only with their parcel ID (like PA-2037)
nw = pd.read_csv(NORWOOD_DATA_PATH + "parcel_id_assn_nr_database.csv",
                 dtype={"assessment_number": str})

# final_df = LT.clean_parcel_id_based_addresses(final_df)
final_df = LT.clean_addresses_with_assessment_number(final_df, lv)
final_df = LT.clean_addresses_with_norwood(final_df, nw)
final_df = LT.clean_ARV_with_landvaluation(final_df, lv)
# Revisit sales data to improve property type
final_df = LT.clean_property_type(final_df, lv)

# Some sales have "ghost" assessment numbers
# https://github.com/bermuda-automation/kw-data-import/issues/5
# remove them
final_df = LT.remove_ghost_assessment_numbers(final_df, lv)

final_df.to_csv("./data/kw-sales.csv", index=False)

print("\n >> LTRO DATA IMPORTED << \n")
