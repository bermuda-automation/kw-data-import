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
                        subfield_list.append(str(subtag.text.encode('utf8')))
                aprop[tag[i].tag] = subfield_list
            else: # has no sub-fields
                if tag[i].text is None:
                    aprop[tag[i].tag] = None
                else:
                    aprop[tag[i].tag] = str(tag[i].text) # .encode('utf8')
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



def clean_assn_nr(an):
    # is the assessment number a 8 or 9 digit string?
    # if it starts by zero, remove the first zero to match the 
    # landvaluation database format.  If it starts by any other number
    # leave it as is.
    # if it contains several numbers separated by comma we will look up
    # their ARV and if the ARV is found, we will keep the assessment number
    # with the highest ARV.  All other situations will be stored as zero.
    pattern = re.compile(r'^\d{8,9}$')
    # regex for a string with only zeroes
    zero_pattern = re.compile(r'^[0]+$')
    an = str(an).strip()

    # it's a single assessment number
    if zero_pattern.match(an):
        return 0
    elif pattern.match(an):
        if len(an) == 9:
            return an
        elif len(an) == 8:
            return '0' + an
        else:
            print('ERROR: assessment number missidenfied', an)
            return 0
    elif len(an) > 9:
        # it may be a list of assessment numbers
        # remove "&" or "and" and other symbols from the lists
        assn_nr = an.replace('b','').replace('[','').replace(']','').replace("'","").strip()
        assn_nr = assn_nr.replace(", and", ",")
        assn_nr = assn_nr.replace(",and", ",")
        assn_nr = assn_nr.replace("&", ",")
        list_of_ass_nr = assn_nr.split(",")
        if len(list_of_ass_nr) <= 1:
            # not a list of assessment numbers
            return 0
        else:
            # process list of assessment numbers
            # remove empty space
            clean_list = [x.strip() for x in list_of_ass_nr]
            # recursively check for assessment numbers (number, length, etc)
            assn_nr_clean = [clean_assn_nr(x) for x in clean_list]
            # keep list elements that are not false
            an_list = [x[0] for x in assn_nr_clean if x]
            if len(an_list) == 0:
                # did we end up with an empty list?
                return 0
            else:
                return an_list
    elif len(an) <= 7:
        return 0
    else:
        print('NOTHING FOUND FOR Assessment Number', an)
        return 0


def clean_address(addr):
    if addr == None:
        return 0
    else:
        try:
            # this should fail
            # the address can't be just a number
            addr = int(addr)
            return 0
        except:
            # it was not a number
            return addr
        

def _fractional_filter(df):
    """
    function to filter dataframe searching for fractional properties
    """
    usual_fractions = [f"1/{n} th" for n in range(2,21)]+[f"1/{n}th" for n in range(2,21)]
    fractional_keywords = ["fraction", "frational", "factional", "fractional", "1/10 share", 
                       "th share", "one tenth", "one sixth", "timeshares", "timeshare", "fractional ownership",
                       "harbour court residences", "harbour court", "tuckers point golf villa",
                       "belmont hills unit", "tucker's point golf villa", "golf villas residence club",
                      "081265514", "081248016", "071919105", "1/10 fraction", "1/10 fractionof"] 
                    # last 3 numbers are the assess_nr of compounds with lots of apartments
                    # like Newstead Belmont Hills, the Reefs, Harbour Court, Tucker's Point
    fractional_keywords = fractional_keywords + usual_fractions

    url = str(df['url']).lower()
    url_condition = (any([x in url for x in fractional_keywords]))

    name = str(df['name']).lower()
    name_condition = (any([x in name for x in fractional_keywords]))
    
    short_desc = str(df['short_description']).lower()
    short_condition = (any([x in short_desc for x in fractional_keywords]))

    long_desc = str(df['long_description']).lower()
    long_condition = (any([x in long_desc for x in fractional_keywords]))

    prop_type = str(df['property_type']).lower()
    type_condition = (any([x in prop_type for x in fractional_keywords]))

    if (url_condition | name_condition | short_condition | long_condition | type_condition):
        return 'fractional'
    elif pd.isnull(df['property_type']):
        return 0
    elif (('081265514' in str(df.assessment_number)) | ('081248016' in str(df.assessment_number)) | \
              ('071919105' in str(df.assessment_number))):
        return 'fractional'
    elif ("Reefs" in str(df.name))  & (df['property_type'] == 'condo'):
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
    elif (int(df.price) < 20_000) & (df.is_sale == 1):
        return "PRICE"   # $20,000 is too cheap for a sale in Bermuda
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
    elif len(str(df.assessment_number)) > 9:
        _an = clean_assn_nr(df.assessment_number)
        if _an != 0:
            # it found a good assessment number or 
            # assessment number list.
            return ""
        else:
            return "ASSN#"
    else:
        return "ASSN#" # if we got here it's not an 8 or 9 character number => likely bad assessment number


def _address_filter(df):
    """ 
    Flag address only if it has no assessment number
    If it has an assessment number then we can match it with the 
    landvaluation database which has accurate addresses.
    if a flag already exists, then append this new one to it.
    """
    if df["name"]: # flag problems with address
        if df["property_type"] == "land" and len(df["name"]) < 10: # for example: 3 South Rd
            # land wont have assessment number to locate it, so a short address is insufficient
            return "ADDRESS"
        elif df["property_type"] == "land" and not contains_number(df["name"]):
            # land won't have assessment number, and an address without number is unlikely to be good
            return "ADDRESS"
        elif df["property_type"] == "fractional" and not contains_number(df["name"]):
            # fractional won't have assessment number, and an address without number is unlikely to be good
            return "ADDRESS"
        elif df["property_type"] == "fractional" and len(df["name"]) < 10:
            return "ADDRESS"
        elif not df["assessment_number"] and ((len(df["name"]) < 10) or not contains_number(df["name"])):
            # no assessment number, short and without number: Unlikely to identify the property
            return "ADDRESS"
        elif len(df["name"]) < 10 and not contains_number(df["name"]): 
            return "ADDRESS" # Address seems too short and has no number
    elif df["name"] and (len(str(df.assessment_number)) == 8) or (len(str(df.assessment_number)) == 9):
        # no address present but single assessment number
        return "" # probably ok to find the address from the assessment number
    elif (len(str(df.assessment_number)) == 8) or (len(str(df.assessment_number)) == 9):
            return ""
    else:
        return "" # probably ok

def _country_filter(df):
    """
    find incorrect country
    (unlikely to be an error, but just in case)
    """
    if pd.isnull(df.country):
        return "bermuda"
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
    flags_country = df.apply(_country_filter, axis =1)

    # apply flag filters
    # note that all filters return strings, so we can concatenate them later
    df["flag"] = flags_address.str.cat(flags_an, sep=" ").str.cat(flags_price, sep=" ").str.cat(flags_country, sep=" ").str.strip()
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
