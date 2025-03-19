import csv
import os
import re

import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd

def download_skipper_xml(xml_file, csv_file):
    """
    Opens local XML, parses it and converts it to CSV
    returns: location and name of CSV file, for example:
    `data/skipper/2022-9-14_skipper_properties.csv`
    """
    tree = ET.parse(xml_file)
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
    csvdata = csv_file # 'data/skipper/{}-{:02d}-{:02d}_skipper_properties.csv'.format(today.year, today.month, today.day)
    
    # writing to csv file
    with open(csvdata, 'w', encoding='utf8') as csvfile: # , encoding='utf8') as csvfile:
        # creating a csv dict writer object 
        writer = csv.DictWriter(csvfile, fieldnames = all_fields)
        # writing headers (field names)
        writer.writeheader()
        # writing data rows
        writer.writerows(all_properties)
    return csvdata

def let_or_rent(df):
    # Currently, in bermuda, we find "is_let = 0" for all properties.
    # this is because bermuda uses the is_rent (US version)
    # in case rent or let exist at the same time or separately:
    rent_or_let = df["is_let"] + df["is_rent"]
    rent_or_let.apply(lambda x: 0 if x < 0 else 1)
    df["is_rent"] = rent_or_let  # now can be 0=not_rent or 1=rent
    return df



def clean_assn_nr(assessment_number):
    """
    Clean up assessment number data which comes in various formats.
    Here are some format examples we encounter in real life:
    [["070730016", "28723423"],
    ["070730016 and 28723423", "234233338"],
    ["0", 0],
    ["0 and 28723423", 0],
    ["0 and 28723423 and 234233338", 0],
    ["0000000", 0],
    ["0000000 and 28723423", 0],
    ["0000000 and 28723423 and 234233338", 0],
    ["0000000 28723423  234233338"],
    ["0000000 and 28723423 and 234233338 and 070730016"],
    ["0000000 and 28723423 and 234233338 and 070730016", "234233336"],
    ["0000000 and 28723423, 234233338 and 070730016, 234233338"],
    "03388888",
    "N/A",
    "03388888 and 070730016",
    "041569016, 041569318 and 041570014"
    "03388888 and 070730016 and 28723423",
    "03388888 and 070730016 and 28723423 and 234233338",
    "03388888 and 070730016 and 28723423 and 234233338 and 070730016",
    "03388888 and 070730016 and 28723423 and 234233338 and 070730016, 234233338",
    "0",
    "Some Property Name without digits",
    "041959019 (Upper Apt), 041960017 (Lower Apt)",
    "030472016, 030472113, 030472210, 030472318, 030472415, 030472512, 030472610, 030472717, 030472814",
    "Dock",
    "020067119,020067216,020067313,021771715",
    "7070587310"
    
    Args:
        input_data: A string or list of strings containing assessment numbers
        
    Returns:
        - 0 if input contains only text without digits
        - 0 if input contains only zeros
        - 0 if all numbers have fewer than 7 digits or more than 10 digits
        - Otherwise, a list of strings with all numbers having 7-10 digits 
          (after removing strings of only zeros)
    """
    # Convert input to list if it's not already
    if not isinstance(assessment_number, list):
        assessment_number = [assessment_number]
    
    # Flatten the list if it contains nested lists
    flat_list = []
    for item in assessment_number:
        if isinstance(item, list):
            flat_list.extend(item)
        else:
            flat_list.append(item)
    
    # Initialize an empty set to store valid assessment numbers
    valid_numbers = set()
    
    # Process each string in the flattened list
    for string in flat_list:
        if not isinstance(string, str):
            continue
            
        # Replace various separators with spaces for consistent parsing
        import re
        string = re.sub(r'\band\b|&|,', ' ', string)
        
        # Extract numbers from parentheses
        parentheses_matches = re.findall(r'\((\d+)\)', string)
        
        # Remove parentheses sections from the string to avoid double counting
        string = re.sub(r'\([^)]*\)', ' ', string)
        
        # Find all remaining potential numbers
        all_numbers = re.findall(r'\b\d+\b', string)
        all_numbers.extend(parentheses_matches)
        
        # Add valid numbers (7-10 digits) to our set
        for num in all_numbers:
            if 7 <= len(num) <= 10:
                valid_numbers.add(num)
    
    # Check if we found nothing but zeros
    if valid_numbers and all(num == '0' or num.strip('0') == '' for num in valid_numbers):
        return 0
    
    # Remove strings containing only zeros
    valid_numbers = {num for num in valid_numbers if not all(c == '0' for c in num)}
    # if we see assessment number with 10 digits, remove the first digit
    # keep all other numbers as is
    valid_numbers = {num[1:] if len(num) == 10 else num for num in valid_numbers}
    
    # If we found valid numbers, return them as a list
    if valid_numbers:
        return list(valid_numbers)
    else:
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
                       "newstead belmont hills",
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

def identify_fractionals(df):
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


def _contains_number(value):
    """Check if the value contains any digits"""
    if pd.isna(value):
        return False
    return bool(re.findall(r'\d+', str(value)))

def _check_assessment_number(row):
    """Check if assessment number is problematic"""
    # Skip fractional and land properties as they don't need assessment numbers
    if row["property_type"] == "fractional" or row["property_type"] == "land":
        return ""
    
    # Check if assessment number is missing or zero
    assn = row["assessment_number"]
    if pd.isna(assn) or assn == 0 or assn == "0":
        return "ASSN#"
    
    return ""

def _check_price(row):
    """Check if price is problematic"""
    if pd.isna(row["price"]):
        return "PRICE"
    elif row["price"] == 0 or row["price"] == "0":
        return "PRICE"
    elif (int(float(row["price"])) < 20_000) and (row["is_sale"] == 1):
        return "PRICE"   # $20,000 is too cheap for a sale in Bermuda
    
    return ""

def _check_address(row):
    """Check if address is problematic"""
    name = str(row["name"]) if not pd.isna(row["name"]) else ""
    
    # No address
    if not name:
        # If we have a valid assessment number, we might be able to find the address later
        # so we don't flag it as an address problem
        if isinstance(row["assessment_number"], str) and len(row["assessment_number"]) in [8, 9]:
            return ""
        return "ADDRESS"
    
    # Property type specific checks
    if row["property_type"] == "land":
        if len(name) < 8 or not _contains_number(name):
            # for example: (3 Road) or (South St.)
            # land wont have assessment number to locate it, so a short address is insufficient
            return "ADDRESS"
    elif row["property_type"] == "fractional":
        if len(name) < 8 or not _contains_number(name):
            # fractional won't have assessment number, 
            # and an address without number is unlikely to be good
            return "ADDRESS"
    else:
        # For other properties
        assn = str(row["assessment_number"]) if not pd.isna(row["assessment_number"]) else ""
        if (assn == "0" or not assn) and (len(name) < 8 or not _contains_number(name)):
            return "ADDRESS"
        elif len(name) < 8 and not _contains_number(name):
            return "ADDRESS"
    
    # other wise, address is probably ok
    return ""

def clean_and_flag_properties(df):
    """
    Generate flags for properties based on assessment number, price, and address issues.
    Returns the dataframe with the flag column populated.
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Initialize flag column
    df["flag"] = ""
    
    # Ensure columns are in the right format
    df["property_type"] = df["property_type"].astype(str).str.lower()
    df["assessment_number"] = df["assessment_number"].astype(str).replace("nan", "0")
    
    # Apply checks
    assessment_flags = df.apply(_check_assessment_number, axis=1)
    price_flags = df.apply(_check_price, axis=1)
    address_flags = df.apply(_check_address, axis=1)
    
    # Combine flags
    for idx in df.index:
        flags = []
        if assessment_flags[idx]:
            flags.append(assessment_flags[idx])
        if price_flags[idx]:
            flags.append(price_flags[idx])
        if address_flags[idx]:
            flags.append(address_flags[idx])
        
        df.loc[idx, "flag"] = " ".join(flags)
    
    # Ensure any empty flag strings are preserved as empty
    df["flag"] = df["flag"].str.strip()
    
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
