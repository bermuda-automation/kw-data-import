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

def contains_number(value):
    ''' check if the value has numbers '''
    return bool(re.findall('[0-9]+', value))
    
def clean_and_flag_properties(csv):

    # create "data frame property skipper" dfps
    dfps = pd.read_csv(csv)
    flag_list = []
