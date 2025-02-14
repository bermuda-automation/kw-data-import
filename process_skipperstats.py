import configparser
import glob, os
from datetime import datetime

import requests 
import numpy as np
import pandas as pd

import utils.skipperstatsutils as SSU
import utils.LTROutils as LT 

# Get secret URL API
keys = configparser.ConfigParser()
keys.read("./utils/kw_config.txt")
url = keys.get("skipperstats", "URL")


################  DOWNLOAD & READ THE DATA ################

# define file to save to
today = datetime.today()
transactions = 'data/skipper/transactions/{}-{:02d}-{:02d}_transactions.xml'.format(today.year, today.month, today.day)
# if file exists for today, don't download again
if not os.path.exists(transactions):
    # Get data from web as XML
    SSU.get_xml_with_wget(url, transactions)

###### Open last downloaded file
xml_downloads = glob.glob('./data/skipper/transactions/*.xml') 
# get the file which was last modified
last_xml_download = max(xml_downloads, key=os.path.getmtime)
print("LATEST XML FILE: {}".format(last_xml_download.split('/')[-1]))

df = SSU.transaction_xml_to_dataframe(last_xml_download)        

# Sales from Skipper Stats
sass = df[df.status == "Sold"]
# delete any empty columns & rows (this removes 0 transactions)
sass = sass.dropna(axis=1, how='all')
sass = sass.dropna(axis=0, how='all')

################  CLEAN DATA ################
# - remove duplicates

# DUPLICATES
# Some ssta entries are duplicated (the only difference is in the pictures they contain)
# here we remove duplicates (we lose some pictures, but hopefully ALL pictures have been imported elsewhere)

sass = sass[~sass.duplicated(subset=['transaction_date', 'parish', 'building_name', \
                            'address_line', 'postcode', 'price','assessment_number'], keep='first')]

print('there are {} sales in Skipper Stats'.format(len(sass)))

# Define a dataframe with the columns we want to compare
# Skipper Stats Sales = sss
sss = sass[['ref','transaction_date','parish', 'building_name', 'address_line', 'postcode', 
            'longitude', 'latitude', 
            'assessment_number', 'price', 'arv_default', 
            'property_type', 'is_land', 'is_fractional_unit', 'photos']].copy(deep=False)

# Create a unique application number as a hash of the address
sss.loc[:, 'application_number'] = sss.apply(lambda x: 'skip-'+ SSU.application_number_hash(x['ref'], 
                                        x['transaction_date'], x['building_name'], x['price']), axis=1)

# move column application_number to first position
cols = sss.columns.tolist() 
cols = cols[-1:] + cols[:-1]
sss = sss[cols]

sss = sss.replace(np.nan, 0, regex=True)
sss['transaction_date'] =  pd.to_datetime(sss['transaction_date'], format='ISO8601').dt.date

# Discard any sales with prices less than $1000
# keep those sales with no price, in case we can retrieve it from LTRO.
# currently 15 transactions have an empty price.
sss = sss[sss.price > 1000]
# Make missing assessment numbers zero
sss.assessment_number = sss.assessment_number.fillna(0)


################  FIX FRACTIONALS & LANDS ################

# change sss.property_type if is_land == 1
sss.loc[sss.is_land == '1', 'property_type'] = 'land'
# change sss.property_type to 'fractiona' if is_fractional_unit == 1
sss.loc[sss.is_fractional_unit == '1', 'property_type'] = 'fractional'

sss = SSU.identify_fractionals(sss)
sss = SSU.identify_lands(sss)

# Some properties are completely unidentified other than by price and date.
# If they are better identified in the kw-sales.csv dataset, 
# we drop them from the skipperstats dataset with an adhoc function.
sss = SSU.drop_unidentified(sss)
sss = SSU.drop_selected_duplicates_by_hand(sss)
# use common naming for parishes:
sss = LT.simplify_parishes(sss)
# add bermuda grid based on lng,lat
sss = SSU.add_bermuda_grid(sss)

# Import Sales from LTRO
sa = pd.read_csv('data/kw-sales.csv', dtype={"assessment_number": str})
# Import Landvaluation Database
lv = pd.read_csv('data/kw-properties.csv', dtype={"assessment_number": str})

# about 14 Skipperstats sales have price=zero and no counter-part in LTRO
# we delete them as they are not useful data.
sss = sss[~(sss.price == 0.0)]  # all sales with non-zero price

#### FIRST DUPLICATES FILTER - primarily based on matching dates ####
sss = SSU.date_filter_for_sss_LTRO_duplicates(sss, sa)

#### SECOND DUPLICATES FILTER - primarily based on Address matching  ####
sss = SSU.address_filter_for_sss_LTRO_duplicates(sss, sa)

#### SECOND DUPLICATES FILTER - primarily based on Fractionals matching  ####
sss = SSU.fractional_filter_for_sss_LTRO_duplicates(sss, sa)
print('\n there are {} new distinct sales from Skipper Stats'.format(len(sss)))

# preare for export with renaming or deleting columns.
sss.drop(['is_land', 'is_fractional_unit'], axis=1, inplace=True)
df = df.rename(columns={'arv_default': 'arv', 'transaction_date': 'registration_date',})

################  FIX NO NAME BUILDINGS ################
sss = SSU.fix_no_name_buildings(sss, lv)

sss.to_csv("./data/kw-skipper-stats-sales.csv", index=False)