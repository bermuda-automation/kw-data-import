import csv
import os
import re

import xml.etree.ElementTree as ET
from datetime import datetime

import requests 
import pandas as pd


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
    datafile = 'data/skipper/{}-{:02d}-{:02d}_skipper_properties.xml'.format(today.year, today.month, today.day)
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
        aprop = {} # a property
        for i in range(nfields):
            if len(tag[i]) > 0:
                # has sub-fields
                subfield_list = []
                for subtag in tag[i]:
                    if subtag.text is None:
                        pass
                    else:
                        subfield_list.append(subtag.text.encode('utf8'))
                aprop[tag[i].tag] = subfield_list
            else: # has no sub-fields
                if tag[i].text is None:
                    aprop[tag[i].tag] = None
                else:
                    aprop[tag[i].tag] = tag[i].text # .encode('utf8')
        all_properties.append(aprop)

    # define file to save to
    today = datetime.today()
    csvdata = 'data/skipper/{}-{:02d}-{:02d}_skipper_properties.csv'.format(today.year, today.month, today.day)
    
    # writing to csv file
    with open(csvdata, 'w', encoding='utf8') as csvfile: # , encoding='utf8') as csvfile:
        # creating a csv dict writer object 
        writer = csv.DictWriter(csvfile, fieldnames = all_fields)
        # writing headers (field names)
        writer.writeheader()
        # writing data rows
        writer.writerows(all_properties)
    # delete downloaded XML file
    os.remove(datafile)
    return csvdata

def let_or_rent(df):
    # Currently, in bermuda, we find "is_let = 0" for all properties.
    # this is because bermuda uses the is_rent (US version)
    # in case rent or let exist at the same time or separately:
    rent_or_let = df["is_let"] + df["is_rent"]
    rent_or_let.apply(lambda x: 0 if x < 0 else 1)
    df["is_rent"] = rent_or_let  # now can be 0=not_rent or 1=rent
    return df



def clean_ass_nr(an):
    if an == None:
        return 0
    elif type(an) == "str" and an.isalpha():
        # assessment number can't be made of letters only
        return 0
    else:
        try:
            _an = int(an)
            if _an == 0:
                return 0
            else:
                # not letters, not None, not zero
                return an
        except:
            # it was not a number
            return 0


def clean_address(addr):
    if addr == None:
        return ""
    else:
        try:
            # this should fail
            # the address can't be just a number
            addr = int(an)
            return ""
        except:
            # it was not a number
            return addr
        

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

                                                            

def contains_number(value):
    ''' check if the value has numbers '''
    return bool(re.findall('[0-9]+', value))


def _price_filter(df):
    """
    function to filter dataframe searching for incorrect prices
    which we will use to flag properties for review
    adds the string "PRICE" to corresponding row of df.flag
    if price is incorrect.
    """
    if pd.isnull(df['price']):
        return "PRICE"
    elif df["price"] == 0 or df["price"] == "0":
        return "PRICE"
    elif (int(df.price) < 2000) & (df.is_sale == 1):
        return "PRICE"   # $2000 is too cheap for a sale in Bermuda
    else:
        return ""  # df['flag']

def _assessment_number_filter(df):
    """
    function to filter dataframe searching for anomalous assessment numbers or addresses
    adds string "Assn Nr" or "Address" if appropriate 
    uniform_property_type(df) function should have run before running this one
    """
    if  df["property_type"] == "land" or df["property_type"] == "fractional":
        return ""
    elif not df["assessment_number"]:
        # not a land, not fractional and ass_nr missing
        return "ASSN#"
    elif df["assessment_number"] == 0 :
        return "ASSN#"
    elif (len(str(df.assessment_number)) == 8) or (len(str(df.assessment_number)) == 9):
         return ""  # probably correct
    else:
        return "ASSN#" # if we got here it's not an 8 or 9 character number => likely bad assessment number


def _address_filter(df):
    """ 
    Flag address only if it has no assessment number
    If it has an assessment number then we can match it with the 
    landvaluation database which has accurate addresses.
    if a flag already exists, then append this new one to it.
    """
    if df["property_type"] == "land" and len(df["name"]) < 12:
        # land wont have assessment number to locate it, so a short address is insufficient
        return "ADDRESS"
    elif df["property_type"] == "fractional" and not contains_number(df["name"]):
        # fractional won't have assessment number, and an address without number is unlikely to be good
        return "ADDRESS"
    elif df["property_type"] == "fractional" and len(df["name"]) < 12:
        return "ADDRESS"
    elif not df["assessment_number"] and ((len(df["name"]) < 12) or not contains_number(df["name"])):
        return "ADDRESS"
    elif (len(str(df.assessment_number)) == 8) or (len(str(df.assessment_number)) == 9):
        return ""
    elif len(df["name"]) < 12: 
        return "ADDRESS" # Address seems too short
    elif not contains_number(df["name"]):
        return "ADDRESS"
    else:
        return "" # probably ok


def _country_filter(df):
    """
    find incorrect country
    (unlikely to be an error, but just in case)
    """
    if pd.isnull(df.country):
        return "COUNTRY"
    elif df.country != "bermuda":
        return "COUNTRY"
    else:
        return df["flag"]

def clean_and_flag_properties(df):
    """
    we use the filters defined above
    to creates columns with flags if there
    are problems with address, assessment number or price
    we then concatenate these columns
    """
    # add column for flags 
    df["flag"] = ""
    # prepare data
    df["property_type"] = df["property_type"].str.lower()
    df["country"] = df["country"].str.lower()

    flags_address = df.apply(_address_filter, axis = 1)
    flags_an = df.apply(_assessment_number_filter, axis =1)  
    flags_price = df.apply(_price_filter, axis =1)

    # apply flag filters
    # note that all filters return strings, so we can concatenate them later
    df["flag"] = flags_address.str.cat(flags_an, sep=" ").str.cat(flags_price, sep=" ").str.strip()
    return df
    

def sanitize_text(df):
    """
    Map carriage returns like \r to \n
    to avoid problems when converting to .csv
    as \r can result in a new line half way through the csv row.
    """
    df.replace('\\n', '', regex=True, inplace=True)
    df.replace('\\r', ' ', regex=True, inplace=True)
    return df                  


def remove_extra_chars(x):
    x_clean = x.replace('b','').replace('[','').replace(']','').replace("'","").strip()
    return x_clean

def clean_up_agent_list(agent_list_string):
        return [remove_extra_chars(x)  for x in agent_list_string.split(',')]
    
def agent_list_to_dict(agent_list):
    if len(agent_list) == 5:
        agent_dict = {
            "id" : agent_list[0],
            "name" : agent_list[1],
            "company" : agent_list[2],
            "email" : agent_list[3],
            "phone" : agent_list[4]
        }
    elif len(agent_list) == 4:
        agent_dict = {
            "id" : agent_list[0],
            "name" : agent_list[1],
            "company" : agent_list[2],
            "phone" : agent_list[3]
        }
    elif len(agent_list) == 6:
        agent_dict = {}
        agent_dict["id"] = agent_list[0]
        agent_dict["name"] = agent_list[1]
        agent_dict["company"] = agent_list[2]
        agent_dict["email"]= [i for i in agent_list if "@" in i][0]
    else:
        agent_dict = {
            "id" : "Unknown",
            "name" : "Unknown",
            "company" : "Unknown",
            "email" : "Unknown",
            "phone" : "Unknown"
        }
    return agent_dict

def simplify_parishes(df):
    '''
    This maps rows with a parish
    - of "City of Hamilton" to the parish "Pembroke"
    - of "Town of St. George" to the parish "St. George's"
    see: https://github.com/bermuda-automation/kw-data-import/issues/4
    and also some errors found in the parish entries from property-skipper
    Unless Property Skipper makes a convention for this field, new inconsistencies
    may appear further down. (I've contacted them to suggest so)
    '''
    df["city"] = df.city.replace(["Town of St. George", "City of Hamilton", "City Of Hamilton",
                                  "CIty of Hamilton", "Hamilton Parish", "St. Georges", "Smith's"], 
                                 ["St. George's", "Pembroke", "Pembroke",
                                  "Pembroke", "Hamilton", "St. George's", "Smiths"], regex=False)
    df.rename(columns = {'city':'parish'}, inplace = True)
    return df
