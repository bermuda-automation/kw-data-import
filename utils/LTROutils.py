# functions for cleaning up LTRO data
import decimal
import re
from operator import itemgetter

import pandas as pd
import numpy as np

import utils.skipperutils as skipu

DATA_PATH = "./data/"
NORWOOD_DATA_PATH = "./data/LTRO/Norwood/"

def clean_ltro_data(df):
    """
    takes a dataframe which has just been imported
    from an LTRO XLSX file and applies some filtering:

    - drop empty rows / columns
    - merge split headers
    - rename headers
    - drop empty prices or prices below $1000
    - drop unidentifiable properties
    - remove time from timestamp (keep only date)
    """

    # delete all empty columns & rows
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
        
    # hack for 2022 as the sheet has yet again different headers
    new_header_list = ['application_number', 'kill', 'sale_type',
                       'kill', 'registration_date', 
                       'kill', 'kill', 'parish', 'kill', 'parcel_area', 'kill',
                       'assessment_number', 'address', 'kill',
                       'Mode of\nAcquisition', 'acquisition_date',
                       'Nature of\nInterest', 'price']
    
    
    
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

    df = df.drop(df[(df.address.str.len() <= 4) & (df.assessment_number == 0)].index)
    df = df.drop(df[(df.address.str.len() <= 4) & (df.assessment_number == "Unknown")].index) 
    # - We will preserve addresses with short code like SM-800/1, DE-1886/A, *, etc
    # The shortest example is SG-09, thus the limit of 4 characters
    # matching those addresses to assessment numbers or addresses is done by function
    # clean_parcel_id_based_addresses() below
    
    # Remove time from the timestamps
    df['registration_date'] =  pd.to_datetime(df['registration_date'], format='%Y-%m-%d %H:%M:%S.%f').dt.date
    
    
    # experimental:
    df.reset_index(drop=True, inplace=True)

    return df

def _in_keywords(x, keywords):
    '''
    Check if any of the keywords are in string x
    - False if it's nan
    - False if value is 0 or "0"
    - True if x can be found in any of the keywords
    '''
    if pd.isnull(x):
        return False
    if x == 0 or x == "0":
        return False
    else:
        return any([keyword in x for keyword in keywords])
            
def identify_fractionals(df):
    '''
    identify which rows correspond to sales of fractional properties.
    These will contain certain keywords and have no assessment number
    input: dataframe (note: must not contain column "property_type" yet)
    output: dataframe
    '''
    usual_fractions = [f"1/{n} th" for n in range(2,21)]+[f"1/{n}th" for n in range(2,21)]
    fractional_keywords = ["fraction", "frational", "factional", "fractional", "1/10 share", 
                       "th share", "one tenth", "one sixth", "timeshares", "timeshare",
                       "harbour court residences", "harbour court", "tuckers point golf villa",
                       "belmont hills unit", "tucker's point golf villa", "golf villas residence club",
                      "081265514", "081248016", "071919105", "1/10 fraction", "1/10 fractionof"] 
                    # last 3 numbers are the assess_nr of compounds with lots of apartments
                    # like Newstead Belmont Hills, the Reefs, Harbour Court, Tucker's Point
    fractional_keywords = fractional_keywords + usual_fractions

    mode_condition = df['Mode of\nAcquisition'].str.lower().apply(lambda x: _in_keywords(x, fractional_keywords))
    ass_nr_condition = df['assessment_number'].str.lower().apply(lambda x: _in_keywords(x, fractional_keywords))
    addr_condition =  df['address'].str.lower().apply(lambda x: _in_keywords(x, fractional_keywords))
    type_condition =  df['Nature of\nInterest'].str.lower().apply(lambda x: _in_keywords(x, fractional_keywords))

    df['property_type'] = ( mode_condition | ass_nr_condition | addr_condition | type_condition )
    # if it's found to be fractional then make sure the property_type is fractional
    df.loc[df.property_type == True, 'property_type'] = 'fractional'
    
    # if it's found to be fractional then make sure the assessment_number is zero.
    # since fractional properties don't have assessment numbers
    df.loc[df.property_type == 'fractional', 'assessment_number'] = 0

    return df

def identify_lands(df, skipper_dataframe=False):
    '''
    Identify which rows correspond to sales of lands
    because they contain certain keywords and have no assessment number
    This function must follow identify_fractionals
    input: dataframe (note: must contain column "property_type".
    output: dataframe
    '''
    land_keywords = ["vacant lot", "lot of land", "land on", "lot", "land lying", 
                 "land situate", "land situated", "share in land", "government land"]
    land_anti_keywords = ["fairyland lane", "fruitland lane", "camelot", "jiblot", "treslot", "3 scenic lane"] # not lands
    
    if skipper_dataframe:
        # for processing propertyskipper data
        address_column_name = 'name'
    else:
        # for processing LTRO data
        address_column_name = 'address'

    df[address_column_name] = df[address_column_name].fillna('')
    df['assessment_number'] = df['assessment_number'].fillna('')

    # Creating conditions for both the keywords and anti-keywords
    keyword_conditions = df[address_column_name].str.lower().apply(lambda x: _in_keywords(x, land_keywords))
    keyword_conditions_assn_nr = df['assessment_number'].str.lower().apply(lambda x: _in_keywords(x, land_keywords))

    anti_keyword_conditions = df[address_column_name].str.lower().apply(lambda x: not _in_keywords(x, land_anti_keywords))
    anti_keyword_conditions_assn_nr = df['assessment_number'].str.lower().apply(lambda x: not _in_keywords(x, land_anti_keywords))

    # Applying both conditions
    df['new_lands'] = (keyword_conditions | keyword_conditions_assn_nr)  & \
                      (anti_keyword_conditions & anti_keyword_conditions_assn_nr)

    df.loc[df.new_lands == True, 'property_type'] = 'land'
    # if it's found to be a land then make sure the assessment_number is zero
    # since lands don't have assessment numbers
    df.loc[df.new_lands == True, 'assessment_number'] = 0

    df = df.drop('new_lands', axis=1)
    return df

def identify_houses(df):
    '''
    identify which rows correspond to sales of house or condo
    based on certain keywords
    input: dataframe (note: must already contain column "property_type")
    output: dataframe
    '''

    house_keywords = ["conveyance", "coveyance"]
    condo_keywords = ["lease", "leashold", "leaseholder", "lese", "leasehodler"]
    condo_addr_keywords = ["lower", "apartment", "unit", "apt.", "apt"]
    
    house_keyword_conditions = df['Mode of\nAcquisition'].str.lower().apply(lambda x: _in_keywords(x, house_keywords))
    house_anti_keyword_conditions = df['Mode of\nAcquisition'].str.lower().apply(lambda x: not _in_keywords(x, condo_keywords))
    house_anti_keyword_conditions_addr = df['address'].str.lower().apply(lambda x: not _in_keywords(x, condo_addr_keywords))

    # a series of True / False depending on if satisfies the conditions to be a house:
    # (has keywords of a house) AND (does not have keywords of a condo) AND (is not fractional)
    df['new_houses'] = (house_keyword_conditions) & \
                       (house_anti_keyword_conditions | house_anti_keyword_conditions_addr) & \
                       (df['property_type'] != 'fractional')
                       
    df.loc[df.new_houses == True, 'property_type'] = 'house'

    # delete temporary column
    df = df.drop('new_houses', axis=1)
    return df

def identify_condos(df):
    '''
    identify which rows correspond to sales of a condo
    based on certain keywords
    input: dataframe (note: must already contain column "property_type")
    output: dataframe
    '''

    condo_keywords = ["lease", "leashold", "leaseholder", "lese", "leasehodler", "assignment"]
    condo_addr_keywords = ["lower", "apartment", "unit", "apt.", "apt"]
    house_keywords = ["conveyance", "coveyance"]
    
    condo_keyword_conditions = df['Mode of\nAcquisition'].str.lower().apply(lambda x: _in_keywords(x, condo_keywords))
    condo_keyword_addr_conditions = df['address'].str.lower().apply(lambda x: _in_keywords(x, condo_addr_keywords))
    condo_anti_keyword_conditions = df['Mode of\nAcquisition'].str.lower().apply(lambda x: not _in_keywords(x, house_keywords))


    # a series of True / False depending on if satisfies the conditions to be a condo:
    # (has keywords of a house) AND (does not have keywords of a house) AND (is not fractional)
    df['new_condos'] = (condo_keyword_conditions | condo_keyword_addr_conditions) & \
                       (condo_anti_keyword_conditions) & \
                       (df['property_type'] != 'fractional')
                       
    df.loc[df.new_condos == True, 'property_type'] = 'condo'

    # delete temporary column
    df = df.drop('new_condos', axis=1)
    return df
    
def process_duplicates(df):
    
    duplis = df[df['application_number'].duplicated(keep=False)]
    current_row = duplis[["application_number","assessment_number", "price"]].iloc[0].tolist()
    processed_rows = []
    marked_for_delete = []

    for index, row in duplis.iterrows():

        current_row = [row.application_number, row.assessment_number, row.price]
        duplis_subset = duplis[duplis["application_number"] == row.application_number]
        an = [skipu.clean_assn_nr(x) for x in duplis_subset['assessment_number']]
        dupli_indx = duplis.index[duplis['application_number'] == current_row[0]].tolist()
        prop_type = duplis_subset.property_type.tolist()

        if index in processed_rows:
            # do nothing in this iteration of the for loop
            # as this row have been proccessed as a group already
            pass

        else:
            # this group of rows needs processing
            # 1. all have assessment nr -> keep all
            # 2. Not all have assessment nr -> ignore ones without
            # 3. if one is fractional, keep only that
            # 4. none have assessment number, keep first with non-empty description.
            has_missing_ass_nr = any([True for x in an if (x == False)])

            # 1. All have ass. nr
            if not has_missing_ass_nr:
                # all entries have assessment number
                final_assn_nr = ','.join([x[0] for x in an])
                final_addr = [x for x in duplis_subset['address']]

                # update original dataframe
                df.loc[dupli_indx,'assessment_number'] = final_assn_nr
                df.loc[dupli_indx,'address'] = final_addr

                # mark duplicates for deletion
                marked_for_delete.extend(dupli_indx[1:])

            # 2. Not all have assn_nr
            if has_missing_ass_nr:
                # some assessment nr missing

                if 'fractional' in prop_type:
                    # 3. keep only fractional
                        which_one_is_fractional = prop_type.index("fractional")
                        to_delete = dupli_indx[0:which_one_is_fractional] + dupli_indx[which_one_is_fractional+1:]
                        marked_for_delete.extend(to_delete)


                elif len(an) == an.count(False) and 'fractional' not in prop_type:
                    # 4. all assessment nr missing
                    # only keep rows without words like "Unknown"
                    unknowns = duplis_subset.assessment_number.tolist()
                    unknowns = list(map(lambda x: str(x).lower(), unknowns))
                    # in which position of our duplicates subset is the unkown assessment number?
                    if 'unknown' in unknowns:
                        index_unknown = unknowns.index("unknown")
                        to_delete = [dupli_indx[index_unknown]]
                    elif '0' in unknowns:
                        index_unknown = unknowns.index("0")
                        to_delete = [dupli_indx[index_unknown]]
                    else:
                        # Are there missing cases with other "unknown" words?
                        print('MISSING ASS NR: ', an, unknowns)
                    
                    # delete rows with some unknown assessment number
                    marked_for_delete.extend(to_delete)

                elif len(an) > an.count(False) and 'fractional' not in prop_type:
                    # some assessment numbers exist, but not all
                    # delete the ones without assessment number
                    which_without_an = [i for i,val in enumerate(an) if val==False]
                    to_delete = [dupli_indx[x] for x in which_without_an]
                    marked_for_delete.extend(to_delete)

            # mark duplicates group as processed:
            processed_rows.extend(dupli_indx)
    
    df = df.drop(marked_for_delete)
    return df

def find_combined_arv(arv_list):
    '''
    Some sales are for the sale of two or more properties, each with their own ARV.
    We calculate here the combined arv. 
    So if the column for arv has [30,000, 40,000] then combined_arv will have [70,000]
    if it has only one ARV value, that value will be kept
    '''
    if type(arv_list) == list:
        if len(arv_list) == 0:
            return 0 # if some lists are empty lists
        elif len(arv_list) > 0:
            return sum(arv_list)
    else:
        try:
            arv_list = int(arv_list)
        except:
            # catches arv_lists which are empty arrays []
            arv_list = 0
        return arv_list

def add_arv_to_ltro(df, lv):
    '''
    input: 
    `df`: dataframe from LTRO (already deduplicated & clean)
    `lv`: dataframe created from land valuation database
    
    output: dataframe with 2 new columns:
    - one with ARVs that match properties from land valuation
    - one with combined ARVs when more than one assessment number matches
    '''
    arvs_for_ltro = []

    for index, row in df.iterrows():
        an = skipu.clean_assn_nr(row.assessment_number)
        if an and len(an) == 1: # single assessment number
            try:
                arv = lv[lv.assessment_number == an[0]].arv.values
            except:
                arv = [0]
            # keep arvs (should be just 1) where something was found
            if len(arv) == 1: 
                arvs_for_ltro.extend(arv)
            elif len(arv) >=1: 
                # this shouldn't happen (more than 1 ARV for a single Ass. Nr.)
                # but just in case
                arvs_for_ltro.append(arv)
            else: # no ARV found
                arvs_for_ltro.append(0)
        elif an and len(an) > 1: # Multiple assessment numbers
            # get ARVs from landvaluation that match those in LTRO
            arvs = [lv[lv.assessment_number == x].arv for x in an]
            # get the value from the dataframe (if it was found, i.e. len(x) > 0)
            arvs = [x.values for x in arvs if len(x) > 0]
            # for example these are not found: ['123075017', '123075211', '123076013', '129077010']
            if len(arvs) > 0:
                # extract the ARV string
                arvsp = [x[0] for x in arvs]
                arvs_for_ltro.append(arvsp)
            else: # no arvs in the list
                arvs_for_ltro.append(0)
        else: # no assessment number
            pass
            arvs_for_ltro.append(0)
        # sanitity check: df.shape[0] should be len(arvs_for_ltro)

    df['arv'] = arvs_for_ltro
    df['combined_arv'] = df.arv.map(lambda x : find_combined_arv(x))

    return df

def clean_property_type(df, lv):
    '''
    Uses land valuation data (more reliable)
    to improve LTRO data (less reliable).
    When land valuation data exists for property type
    with the same assessment number it is used to correct
    the property type from LTRO.
    ! Assumes assessment_number has already been processed
    ! This function runs twice in process_LTRO, to catch wrong an
    `input`: Dataframes from LTRO (df) and from Landvaluation (lv)
    `output`: processed LTRO sales dataframe (df)
    '''
    for index, row in df.iterrows():
        an = row.assessment_number
        if isinstance(an, list) and len(an) == 1:
            # a single assessment number
            # find matching landvaluation property_type
            # should only have one match as lv assessment numbers
            # should uniquely match a property.
            p_type = lv[lv.assessment_number == an[0]]
            if p_type.shape[0] > 0:
                p_type = str(p_type.property_type.values[0])
            else:
                p_type = 0
        elif isinstance(an, list) and len(an) > 1:
            # several assessment numbers
            p_types = set()
            for i in an:
                a_p_type = lv[lv.assessment_number == i]
                if a_p_type.shape[0] > 0:
                    p_types.add(str(a_p_type.property_type.values[0]))
                else:
                    # no assessment number found to match
                    pass
            if len(p_types) == 0:
                # no matching property type found
                p_type = 0
            elif len(p_types) == 1:
                # one or serveral assessment numbers match
                # but all have the same property_type
                p_type = list(p_types)[0]
            if len(p_types) > 1:
                # several assessment numbers have
                # several property types
                # If there are several, keep the property type with the higher assessment number
                # retrieve the arv for each of the assessment numbers
                list_of_arv_assn_nr_and_p_type = []
                for one_an in an:
                    assn_nr_match = lv[lv.assessment_number == one_an]
                    if assn_nr_match.shape[0] > 0:
                        # make a list with an, arv and property_type
                        list_of_arv_assn_nr_and_p_type.append((one_an, assn_nr_match.arv.values[0], assn_nr_match.property_type.values[0]))

                # keep property_type associated with highest Assn_nr
                p_type = max(list_of_arv_assn_nr_and_p_type, key=itemgetter(1))[2]
        elif an == 0:
            p_type = 0
        elif an == '0':
            p_type = 0
        else:
            # some assessment numbers are mis-identified
            # as strings instead of lists. Fix them.
            # Since this function runs twice, the property type
            # will be found the second time
            fixed_an = skipu.clean_assn_nr(an)

        # does the df already have a property type?
        current_p_type = row.property_type
        if p_type == 0 and current_p_type == False:
            # change to zero
            df.loc[index, 'property_type'] = p_type
        elif p_type == 0 and isinstance(current_p_type, str):
            pass # keep as it is
        elif p_type !=0 and current_p_type == False:
            # replace with landvaluation type
            df.loc[index, 'property_type'] = p_type
        elif p_type !=0 and current_p_type == 0:
            # replace with landvaluation type
            df.loc[index, 'property_type'] = p_type
        elif p_type !=0 and isinstance(current_p_type, str):
            # trust the landvaluation more
            df.loc[index, 'property_type'] = p_type
        else:
            print('No property types found for ----->', p_type, current_p_type)
    return df   

def clean_area(df):
    # clean_area
    # 1. is there a number?
    # 2. is there a unit?
    # if not save zero, otherwise convert to Ha and store
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
            
    # Create New Dataframe Column
    df['parcel_area_ha'] = unified_area
    return df

def nr_of_decimals(x):
    '''
    returns the number of decimals in a float:
    2.3 -> 1
    4.455 -> 3
    '''
    d = decimal.Decimal(x)
    return -d.as_tuple().exponent

def smtm(x, show_decimals=False):
    ''' Show me the money!
    return a string with currency fomat'''
    if show_decimals == True:
        if x < 1 and nr_of_decimals(x)==1:
            return "${:,.1f}".format(x)
        elif x<1 and nr_of_decimals(x)==2:
            return "${:,.2f}".format(x)
        else:
            return "${:,.0f}".format(x)
    else:
        return "${:,.0f}".format(x)

def get_units(area_string):
    '''
    checks if any of the standard surface area units
    are present and returns them
    :param str area_string: could be something like "0.222 hectare/0.550 acre"
    0.264 ha. (0.652 ac.)
    :return: list of units in the order they appear
    :rtype: list
    '''
    possible_units = ['ac', 'acre', 'ha', 'sq m', 'square meter', 'hectare', 'sq ft', 'square feet']
    # minor bug: 'acre' is read as having 'ac', but it's the correct unit anyway
    unit_list = [x for x in possible_units if x in area_string]
    return unit_list

def convert_to_ha(value, unit):
    '''
    converts units of input to ha
    works for acre, ac, sq m, square meter, hectare, sq ft
    '''
    if unit == 'ha' or unit == 'hectare':
        try:
            return float(value)
        except:
            return None
    if unit == 'ac' or unit == 'ac':
        try:
            new_value = float(value)*0.404686
            return new_value
        except:
            return None
    if unit == 'sq m' or unit == 'square meter':
        try:
            new_value = float(value)*0.0001
            return new_value
        except:
            return None
    if unit == 'sq ft' or unit =='square feet':
        try:
            new_value = float(value)*9.2903/1000000
            return new_value
        except:
            return None

def simplify_parishes(df):
    '''
    This maps rows with a parish
    - of "City of Hamilton" to the parish "Pembroke"
    - of "Town of St. George" to the parish "St. George's"
    see: https://github.com/bermuda-automation/kw-data-import/issues/4
    '''
    if 'city' in df.columns:
        # this works for propertyskipper data
        df["city"] = df.city.replace(["Town of St. George", "City of Hamilton", "City Of Hamilton",
                                  "CIty of Hamilton", "Hamilton Parish", "St. Georges", "Smith's"], 
                                 ["St. George's", "Pembroke", "Pembroke",
                                  "Pembroke", "Hamilton", "St. George's", "Smiths"], regex=False)
    elif 'parish' in df.columns:
        # this works for landvaluation or other data
        df["parish"] = df.parish.replace(["Town of St. George", "City of Hamilton", "City Of Hamilton",
                                  "CIty of Hamilton", "Hamilton Parish", "St. Georges", "Smith's"], 
                                 ["St. George's", "Pembroke", "Pembroke",
                                  "Pembroke", "Hamilton", "St. George's", "Smiths"], regex=False)
    else:
        print("ERROR: No column named 'city' or 'parish' found in dataframe")
    return df
        
def clean_parcel_id_based_addresses(df):
    """
    DEPRECATED IN FAVOUR OF TWO OTHER FUNCTIONS:

        1. clean_addresses_with_assessment_number(df, lv)
        and
        2. clean_addresses_with_norwood(df, nw):

    Attempts to identify properties which have an address
    less than 10 char long based on Norwood dataset
    for example will match: PE-3121 => 3 Jane Doe Lane
    """
    dfclean = df.copy(deep=False)
    nw = pd.read_csv(NORWOOD_DATA_PATH + "parcel_id_assn_nr_database.csv", dtype={"assessment_number": str})
    lv = pd.read_csv(DATA_PATH + "kw-properties.csv", dtype={"assessment_number": str})

    addr_matches = []
    addr_n_assn_nr_matches = []
    no_match = []

    
    for df_index, row in dfclean.iterrows():
        assn_str = str(row.assessment_number)
        addr = str(row.address)
        # we convert to str as some "0" int are somehow in it.
        
        if (len(addr) < 10) and (assn_str != "False"):
            # address is a parcel ID, but we have the assessment number
            # find the address in the landvaluation file
            # 1. sanity check on assessment number format
            if (assn_str[0:2] == "['") and (assn_str[-2:] == "']"):
                assn_str = assn_str.strip()
                # extract assn numbers from string
                for char in [  "['",   "'",   "']",
                               '"',    " ",    "[",   "]" ]:  
                    assn_str = assn_str.replace(char, "")
                assn_strs = assn_str.split(',')
                assn_list = []
                # create list with assessment numbers found
                # verify that they are 9 digits long
                # else add a trailing zero to the 8 digit long assessment number
                for _an in assn_strs:
                    if len(_an) == 9:
                        assn_list.append(_an)
                    if len(_an) == 8:
                        _an = "0" + _an
                        assn_list.append(_an)
                    else:
                        print('ERROR: Problem extracting assessment number for', row.assessment_number)
                    # find it in the land valuation (lv dataframe)
                addresses = ""
                for j, assn_x in enumerate(assn_list):
                    addresses += lv[lv.assessment_number == assn_x]["building_name"].values[0]
                    addresses += ", " + lv[lv.assessment_number == assn_x]["address"].values[0]
                    if j < len(assn_list)-1:
                        addresses += " + " # separate multiple addresses
                        addresses
                        # print(assn_list, '---->\n', addresses, '\n\n')
                dfclean.loc[df_index, 'address'] = addresses
                # keep list of addresses found
                addr_n_assn_nr_matches.append(addr)
            else:
                print("ERROR: Problem extracting assessment number for", row.address)
        elif (len(addr) < 10) and (addr != "0") and (assn_str == "False"):
            # address is parcel ID but we don't have an assessment number
            street_match = nw[nw.parcel_id == row.address].street_address

            if len(street_match) == 1:
                # the address matches on known parcel_id
                dfclean.loc[df_index, 'address'] = street_match.iat[0]
                assn_nr_match = nw[nw.parcel_id == row.address].assessment_number
                dfclean.loc[df_index, 'assessment_number'] = assn_nr_match.iat[0]
                addr_n_assn_nr_matches.append(addr)

            elif len(street_match) < 1:
                # no address matches this parcel_id
                if "/" in addr:
                    # try again without the part after the slash
                    street_matches = row.address.split('/')
                    street_match = nw[nw.parcel_id == street_matches[0]].street_address
                    if len(street_match) == 1:
                        # the address without "/" matches a known parcel_id
                        # keep the first hit
                        potential_address = "{} ({})".format(street_match.iat[0], 
                                                            street_matches[1]) 
                        dfclean.loc[df_index, 'address'] = street_match.iat[0]
                        addr_matches.append(addr)
                    elif len(street_match) > 1:
                        print('Multiple Matches from Norwood:', street_match)
                else:
                    # print("####=> nothing found for", row.address)
                    no_match.append(addr)
            elif len(street_match) > 1:
                # several addresses match this parcel_id
                print("several addresses match this parcel_id. Investigate")
                print('---------------------->', row.address, street_match)
                no_match.append(addr)

    print("\nProcessing Parcel_IDs ...\n")
    print("Address and assessment number found for: ", addr_n_assn_nr_matches)
    print("Address found for: ", addr_matches)
    print("No match found for: ", no_match, "\n")
    # and print it with friendly message
    return dfclean

def clean_addresses_with_assessment_number(df, lv):
    ''' 
    Address is deficient, but assessment number is present.

    we will trust the assessment number before the parcel_id
    if several assessment_numbers match and the address is the same,
    we will keep both building names
    :param df: dataframe with deficient addresses
    :param lv: landvaluation dataframe
    return: dataframe df with updated addresses
    '''

    deficient_addresses_with_an = df[(df.address.astype(str).str.len() < 10) & (df.assessment_number != 0)]

    for k, row in deficient_addresses_with_an.iterrows():
        # grab assessment numbers
        _anl = row.assessment_number 
        matches = []
        for an in _anl:
            # loop over assessment numbers and look them up in the 
            # landvaluation dataset.
            lv[lv.assessment_number == an]
            matches.append(lv[lv.assessment_number == an])
        
        if len(matches) == 1: # one match
            new_building_name = matches[0].building_name.values[0]
            if new_building_name == '': # emptry building name
                new_addr = matches[0].address.values[0]
            else:
                new_addr = matches[0].building_name.values[0] + ', ' + matches[0].address.values[0]
        elif len(matches) > 1: # several matches
            # are all matches for the same address?
            _multi_addr = set([matches[i].address.values[0] for i in range(len(matches))])
            if len(_multi_addr) == 1: # yes, same address
                # join all distinct building names associated with that address
                # that were in our list of assessment numbers
                new_building_name = ', '.join(set([matches[i].building_name.values[0] for i in range(len(matches))]))
                new_addr = new_building_name + ', ' + matches[0].address.values[0]
            if len(_multi_addr) == 1: # several addresses (change to > 1)
                # let's keep only the address with the largest ARV
                arv_values = [matches[i].arv.values[0] for i in range(len(matches))]
                max_arv = arv_values.index(max(arv_values))
                new_building_name = matches[max_arv].building_name.values[0]
                new_addr = new_building_name + ', ' + matches[max_arv].address.values[0]

        # update dataframe with the new values
        df.loc[k, 'address'] = new_addr
    return df

def clean_addresses_with_norwood(df, nw):
    '''
    2. Address is defficient and assessment number is missing.

    let's identify those addresses which are short codes
    or parcel IDs.  They are usually between 4 - 11 characters.
    they begin with a 2 letter code for the parish, followed by 2 - 4 numbers.
    shortest has form: PA-8, longest has form: SO-001814
    :param df: dataframe with addresses
    :param nw: norwood dataframe
    :return: dataframe with addresses cleaned
    '''
    pattern = re.compile(r'^[A-Z]{2}-\d{1,6}$')
    deficient_addresses_no_an = df[(df.address.astype(str).str.match(pattern)) & (df.assessment_number == 0)]
    addr_matches = []
    no_match = []


    for k, row in deficient_addresses_no_an.iterrows():
        # look up address in norwood dataset
        addr = row.address
        match = nw[nw.parcel_id == addr]
        if match.shape[0] > 0:
            new_str = match.street_address.values[0]
            new_parish = match.parish.values[0]
            new_postcode = match.postcode.values[0]

            new_addr = "{}, {}, {} ({})".format(new_str, new_parish, new_postcode, addr)
            assn_nr_match = match.assessment_number.values[0]
            addr_matches.append(addr)

        if match.shape[0] == 0:
            # no address matches this parcel_id
            if "/" in addr:
                # try again without the part after the slash
                shorter_addr = row.address.split('/')
                match = nw[nw.parcel_id == shorter_addr[0]]
                if len(match) == 1:
                    new_str = match.street_address.values[0]
                    new_parish = match.parish.values[0]
                    new_postcode = match.postcode.values[0]

                    new_addr = "{}, {}, {} ({})".format(new_str, new_parish, new_postcode, addr)
                    assn_nr_match = match.assessment_number.values[0]
                    addr_matches.append(addr)
                elif len(match) > 1:
                            print('Multiple Matches from Norwood:', match)
                else:
                    print("####=> nothing found for", row.address)
                    no_match.append(addr)
                    assn_nr_match = 0
            else:
                no_match.append(addr)
                assn_nr_match = 0
        
        df.loc[k,'address'] = new_addr
        if assn_nr_match != '0' and assn_nr_match != 0:
            if len(assn_nr_match.split(',')) <= 2:
                df.loc[k, 'assessment_number'] = assn_nr_match
            else:
                # too many assessment numbers associated with that value
                pass

    print("\nProcessing Parcel_IDs ...\n")
    print("Address found for: ", addr_matches)
    print("No match found for: ", no_match, "\n")
    return df
