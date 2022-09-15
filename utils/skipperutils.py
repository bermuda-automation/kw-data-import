import csv
import os
import re

import xml.etree.ElementTree as ET
from datetime import datetime

import requests 
import pandas as pd
import numpy as np

def download_skipper_xml(url):
    """
    Opens url with API endpoint which is an XML file
    downloads XML, parses it and converts it to CSV
    returns: location and name of CSV file, for example:
    `data/skipper/2022-9-14_skipper_properties.csv`
    """

    # Get data from web as XML
    resp = requests.get(url)
    property_data = resp.content
    
    # define file to save to
    today = datetime.today()
    datafile = 'data/skipper/{}-{}-{}_skipper_properties.xml'.format(today.year, today.month, today.day)
    # Save data to local file
    with open(datafile, 'wb') as f:
        f.write(resp.content)
        # create element tree object
    tree = ET.parse(datafile)
    # get root element
    root = tree.getroot()
    # read XML and save to CSV
    most_nr_of_fields = 0
    for tag in root.findall('property'):
        nfields = len(tag)
        if nfields > most_nr_of_fields:
            most_nr_of_fields = nfields
            # keep list of fields from property
            # with the longest number of fields
            for i in range(nfields):
                all_fields = [x.tag for x in tag]
                
    extracted_fields = "','".join(all_fields)
    all_fields = "'" + extracted_fields + "'"
    sfields = all_fields.split(",")
    all_fields = [x[1:-1] for x in sfields]

    all_properties = []
    for tag in root.findall('property'):
        nfields = len(tag)
        aprop = {}
        for i in range(nfields):
            if len(tag[i]) > 0:
                # has sub-fields
                subfield_list = []
                for subtag in tag[i]:
                    if subtag.text is None:
                        pass
                    else:
                        subfield_list.append(subtag.text) # .encode('utf8'))
                aprop[tag[i].tag] = subfield_list
            else: # has no sub-fields
                if tag[i].text is None:
                    aprop[tag[i].tag] = None
                else:
                    aprop[tag[i].tag] = tag[i].text # .encode('utf8')
        all_properties.append(aprop)

    # define file to save to
    today = datetime.today()
    csvdata = 'data/skipper/{}-{}-{}_skipper_properties.csv'.format(today.year, today.month, today.day)
    
    # writing to csv file
    with open(csvdata, 'w', encoding='utf8') as csvfile:
        # creating a csv dict writer object 
        writer = csv.DictWriter(csvfile, fieldnames = all_fields)
        # writing headers (field names)
        writer.writeheader()
        # writing data rows
        writer.writerows(all_properties)
    # delete downloaded XML file
    os.remove(datafile)
    return csvdata


def _fractional_filter(df):
    """
    function to filter dataframe searching for fractional properties
    """
    if pd.isnull(df['property_type']):
        return 0
    elif ('fractional' in str(df.name)):
        return 'fractional'
    elif (("Tucker's Point" in str(df.name)) | ("Newstead" in str(df['name'])) |
   ("Reefs" in str(df.name)))  & (df['property_type'] == 'condo'):
        return 'fractional'
    else:
        return df['property_type']

def identify_land_and_fractional(df):
    """
    Find properties which may have the wrong property type
    In particular those which don't have assessment number
    because they were actually land or fractional
    """
    df.property_type = df.apply(_fractional_filter, axis = 1)
    return df


def contains_number(value):
    ''' check if the value has numbers '''
    return bool(re.findall('[0-9]+', value))

def clean_and_flag_properties(csv):
    '''
    Create a dataframe from the csv file and then,
    Flag properties:
    - without assessment number
    - without address
    - with the wrong country
    '''
    dfps = pd.read_csv(csv)
    flag_list = []
    for index, row in dfps.iterrows():
        an = row.assessment_number
        nam = str(row["name"])
        
        if type(an) == float:
            # catch assessment_numver == nan
            flag_list.append("FLAG - Assn #")
        elif (len(an) == 8) or (len(an)== 9):
            # has a valid assessment number
            if (contains_number(nam)) and (len(nam) > 20):
                # probably has a valid address
                # at least it is long and has a number
                flag_list.append(0) # 0 = no flag
            elif str(an) == "000000000" or str(an) == "00000000":
                flag_list.append("FLAG - Assn #")
            else:
                # if it either
                # - has a number but is short
                # - has no number
                # it probably needs to be reviewed
                flag_list.append("FLAG - Addr")
                
        elif row["country"].lower() != "bermuda":
            flag_list.append("FLAG - Country")
        else:
            flag_list.append("FLAG - Assn #")
    dfps["flag"] = flag_list

    # Currently in bermuda we find all "is_let = 0".
    # in case rent or let exist at the same time or separately:
    rent_or_let = dfps["is_let"] + dfps["is_rent"]
    rent_or_let.apply(lambda x: 0 if x < 0 else 1)
    dfps["is_rent"] = rent_or_let  # now can be 0=not_rent or 1=rent
    return dfps

def uniform_property_type(df):
    """
    Map the diversity of property types to only a few categories
    Identify and label the fractional properties
    """
    # Load Dictionary from CSV
    with open('data/property_type_dict.csv') as csv_file:
        reader = csv.reader(csv_file)
        property_type_dict = dict(reader)
    # map some property types
    df["property_type"] = df["property_type"].str.lower()
    df2=df.replace({"property_type": property_type_dict})

    #
    return df2

