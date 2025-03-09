# functions for cleaning up LTRO data
import decimal
import re
from operator import itemgetter

import pandas as pd
import numpy as np
from thefuzz import fuzz

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
    # AND
    # - have assessment number "unknown" or "0"

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
    # we cast the column to a string type (to avoid FutureWarning about 
    # boolean and strings being incompatible in the same column)
    df['property_type'] = df['property_type'].astype('str')
    df.loc[df.property_type == 'True', 'property_type'] = 'fractional'
    
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

def remove_application_number_duplicates(df):
    '''
    Although application numbers should be unique to each sale,
    There are exceptions, where the acquisition date matches,
    and most other parameters match, but the registration date
    and application number has two different values.  We will
    delete one entry and keep the latest registration date.
    We will check the following to make the decision:
    - acquisition date matches
    - assessment number matches
    - price matches
    - address has a high fuzzy match
    This function is somewhat adhoc to cope with the lack of 
    consistency in the data entry of LTRO.
    '''
    sa_duplicates = df[df.duplicated(subset=['assessment_number', 
                                             'parish', 
                                             'price', 
                                             'acquisition_date', 
                                             'property_type'], 
                                             keep=False)].sort_values(by="acquisition_date")
    seen_indices = []
    to_delete = []
    for k, row in sa_duplicates.iterrows():
        if k in seen_indices:
            continue
        dupli = sa_duplicates[(sa_duplicates.assessment_number == row.assessment_number) \
                              & (sa_duplicates.price == row.price)]
        if len(dupli) <= 1:
            # something happened here
            # no duplicates found but should be.
            continue
        if len(dupli) == 2:
            # confirm that the addresses are a close match
            similarity_ratio = fuzz.ratio(dupli.address.values[0],
                                        dupli.address.values[1])
            # we assume duplicates are only two
            if similarity_ratio > 80:
                # they are close enough we can remove the first one
                to_delete.append(dupli.index[0])
                seen_indices.append(dupli.index[1])
            else:
                trunc_similarity_ratio = fuzz.ratio(dupli.address.values[0][15:],
                                        dupli.address.values[1][15:])
                if trunc_similarity_ratio > 80:
                    # although the first characters don't coincide,
                    # they are close enough we can remove the first one
                    to_delete.append(dupli.index[0])
                    seen_indices.append(dupli.index[1])
                    
                else:
                    # perhaps fuzzy match is far but end of address and numbers match?
                    end_similarity_ratio = fuzz.ratio(dupli.address.values[0][-25:],
                                                    dupli.address.values[1][-25:])
                    if end_similarity_ratio > 85:
                        # extract only the numbers using regular expressions
                        numbers_only_0 = set(re.findall(r'\d+', dupli.address.values[0]))
                        numbers_only_1 = set(re.findall(r'\d+', dupli.address.values[1]))
                        # print the result
                        if numbers_only_1 == numbers_only_0:
                            # both addresses match at the and and contain the same numbers
                            to_delete.append(dupli.index[0])
                            seen_indices.append(dupli.index[1])

        else:
            # too many matches?
            # can we match with the address and date?
            addr_dupli = dupli[(dupli.address == row.address) & (dupli.acquisition_date == row.acquisition_date)]
            if len(addr_dupli) == 2:
                to_delete.append(dupli.index[0])
                seen_indices.append(dupli.index[1])

    print(len(to_delete), 'LTRO Application number duplicates processed')
    df = df.drop(to_delete)
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
    df['arv'] = ''
    for k, row in df.iterrows():
        an = skipu.clean_assn_nr(row.assessment_number)
        if an and len(an) == 1: # single assessment number
            try:
                arv = lv[lv.assessment_number == an[0]].arv.values
                if len(arv) == 0:
                    df.at[k, 'arv'] = 0
                else:
                    df.at[k, 'arv'] = [arv[0]]
            except:
                df.at[k, 'arv'] = 0

        elif an and len(an) > 1: # Multiple assessment numbers
            # get ARVs from landvaluation that match those in LTRO
            arvs = [lv[lv.assessment_number == x].arv for x in an]
            # get the value from the dataframe (if it was found, i.e. len(x) > 0)
            arvs = [x.values for x in arvs if len(x) > 0]
            # for example these are not found: ['123075017', '123075211', '123076013', '129077010']
            if len(arvs) > 0:
                # extract the ARV string
                arvsp = [x[0] for x in arvs]
                df.at[k, 'arv'] = arvsp
            else: # no arvs in the list
                df.at[k, 'arv'] = 0
        else: # no assessment number
            df.at[k, 'arv'] = 0

    df['combined_arv'] = df.arv.apply(find_combined_arv)
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
            df['assessment_number'] = df.assessment_number.apply(skipu.clean_assn_nr)

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
    for _, row in df.iterrows():
        si = str(row.parcel_area).lower()
        
        if si != 'nan':
            contains_digit = any(map(str.isdigit, si))
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
            if new_building_name == '' or new_building_name == '\xa0': # emptry building name
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
    pattern2 = re.compile(r'^[A-Z]{2}-\d{1,6}/[A-Z]$')
    pattern3 = re.compile(r'^[A-Z]{2}-\d{1,6}/\d{1}$')
    deficient_addresses_no_an = df[(df.address.astype(str).str.match(pattern) | \
                                    df.address.astype(str).str.match(pattern2) | \
                                    df.address.astype(str).str.match(pattern3)) & \
                                   (df.assessment_number == 0)]
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
                    print(f"####=> nothing found for {row.address} using Norwood")
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


def clean_ARV_with_landvaluation(df, lv):
    """
    This function should run following 
    clean_address_with_norwood(df, nw).
    It will replace missing ARVs with those from landvaluation
    This function is applied after new assessment numbers
    have been added from Norwood.
    """
    for k, row in df.iterrows():
        current_arv = row.arv
        if current_arv == '0' or current_arv == 0:
            an = row.assessment_number
            if an != 0 and an != '0':
                if len(an) == 1 and type(an) == list:
                    lv_arv = lv[lv.assessment_number == an[0]].arv.values
                    if len(lv_arv) == 1:
                        df.at[k, 'arv'] = lv_arv
                elif len(an) > 1 and type(an) == str:
                    clean_an = an.replace('[', '').replace(']', '').replace("'", '').strip()
                    clean_an = [x.strip() for x in clean_an.split(',')]
                    if len(clean_an) == 1:
                        lv_arv = lv[lv.assessment_number == clean_an[0]].arv.values
                        if len(lv_arv) == 1:
                            # one ARV has been found in landvaluation
                            # corresponding to this assessment number
                            df.at[k, 'arv'] = lv_arv
                    elif len(clean_an) > 1:
                        arv_list = []
                        for an_an in clean_an:
                            lv_arv = lv[lv.assessment_number == an_an].arv.values
                            if len(lv_arv) == 1:
                                arv_list.append(lv_arv[0])
                        if len(arv_list) > 0:
                            df.at[k, 'arv'] = arv_list            
    return df

def remove_ghost_assessment_numbers(df, lv):
    """
    many LTRO sales contain assessment numbers which cannot be found 
    anywhere else at landvaluation (even old dataset). These "ghost" 
    assessment numbers provide no information and no value.

    This function removes assessment numbers when the number of 
    assessment numbers in a list doesn't match the number of ARVs
    and those assessment numbers can't be found in landvaluation

    We finally remove sales with assessment number 0 and address 0.
    """
    
    number_of_assessment_numbers = (df.assessment_number
                                     .astype(str)
                                     .str.split(',').apply(len)
                                     .value_counts().index
                                    )
    print('# of ARVs does not match # of assessment numbers')
    print('for {} sales'.format(sum(number_of_assessment_numbers)))
            
    for n_of_an in number_of_assessment_numbers:
        # dataframe showing those sales for which the number of assessment numbers does not match the number of ARVs
        unmatched_nr_of_an_arv = df[(df.assessment_number.astype(str).str.split(',').apply(len) == n_of_an) & \
                                    (df.arv.astype(str).str.split(',').apply(len) != n_of_an)]
        if unmatched_nr_of_an_arv.shape[0] > 0:
            for i, row in unmatched_nr_of_an_arv.iterrows():
                if type(row.assessment_number) == list:
                    an_clean_list = row.assessment_number
                else:
                    an_list = row.assessment_number.split(',')
                    an_clean_list = [x.replace("[","").replace("]","").replace("'","").strip() for x in an_list]
                for an in an_clean_list:
                    lv_match = lv[lv.assessment_number == an]
                    if lv_match.shape[0] == 0:
                        an_clean_list.remove(an)
                # update the original sa dataframe with the new assessment number list
                df.at[i, 'assessment_number'] = str(an_clean_list)
    print('Removing "ghost" assessment numbers ...')
    # remove sales with assessment number 0 and address 0
    df = df[~((df.assessment_number == '0') & (df.address == '0'))]
    df = df[~((df.assessment_number == 0) & (df.address == 0))]

    return df


def _list_from_assessment_number_string(an):
    assn_nr = an.replace('[','').replace(']','').replace("'", "").strip()
    assn_nr_list = [a.strip() for a in assn_nr.split(",")]
    return assn_nr_list

def _fuzzy_address_match(addr1, addr2):
    similarity_ratio = fuzz.ratio(addr1, addr2)
    return similarity_ratio

def clean_addresses_with_landvaluation(df, lv):
    """
    If an LTRO sale has a single assessment number, we will
    substitute the address with the proper landvaluation one.
    If an LTRO sale has several assessment numbers, we will
    create a full_address with all addresses from landvaluation which
    match the assessment numbers.
    If an LTRO sale has no assessment number, we will try to find
    a fuzzy match to a landvaluation address and try to substitute it then
    THIS MAY SUPERSEDE clean_addresses_with_assessment_number
    """

    # define a full address to later store the normative address
    df["full_address"] = df["address"]

    for k, row in df.iterrows():
        an = row.assessment_number

        # make sure it comes in list form
        if type(an) == str:
            assn_nr_list = _list_from_assessment_number_string(an)
        elif type(an) == list:
            assn_nr_list = an
        elif an == 0:
            assn_nr_list = ['0']

        if assn_nr_list[0] != '0':
            # use assessment number to find a well defined address from lv.
            if len(assn_nr_list) == 1:
                # The sale has a single assessment number
                lv_an_match = lv[lv.assessment_number == assn_nr_list[0]]

                if len(lv_an_match) == 1:
                    # single match
                    if lv_an_match.building_name.values[0] == '\xa0':
                        lv_addr = lv_an_match.address.values[0]
                    else:
                        lv_addr = f"{lv_an_match.building_name.values[0]}, {lv_an_match.address.values[0]}"
                    if _fuzzy_address_match(lv_addr, row.address) > 58:
                        # replace LTRO address with the normative address from landvaluation
                        df.loc[k, 'address'] = lv_addr
                        df.loc[k, 'full_address'] = lv_addr
                        df.loc[k, 'arv'] = lv_an_match.arv.values
                        df.loc[k, 'combined_arv'] = lv_an_match.arv.values.sum()
                    else:
                        # keep address for the full address
                        df.loc[k, 'full_address'] = row.address
                        pass
                        # leave address as is. (This seems like an LTRO error, 
                        # address and assessment number don't match, 
                        # but we don't know which is wrong)
                elif len(lv_an_match) > 1:
                    # multiple matches in landvaluation
                    # this should not happen
                    print("[WARNING] ==> Landvaluation properties have duplicates")
                    print("This should not happen. Check the lv code")
                elif len(lv_an_match) == 0:
                    pass
                    # LTRO probably input the assessment number incorrectly. 
                    # so there is no assn_nr match on the lv database.
                    # We just ignore them (although the addresses do appear in lv).
                    df.loc[k, 'full_address'] = row.address
                else:
                    print(f"\n===> No match for {row.address} | an: {an}\n")
            elif len(assn_nr_list) > 1:
                # the sale has multiple assessment numbers
                lv_an_match = lv[lv.assessment_number.isin(assn_nr_list)]
                # create a full address which combines the multiple properties
                # associated with each assessment number in the sale
                addresses_and_buildings = [f"{bu}, {ad}" for ad,bu in zip(lv_an_match.address, lv_an_match.building_name)]
                full_address = "\n".join(addresses_and_buildings)
                if len(full_address) > 1:
                    df.loc[k, 'full_address'] = full_address
                    df.loc[k, 'arv'] = str([int(x) for x in lv_an_match.arv.values])
                    df.loc[k, 'combined_arv'] = lv_an_match.arv.values.sum() 
                else:
                    # no match found in landvaluation
                    # so no address is obtained from lv_an_match.
                    # we keep the address as is.
                    df.loc[k, 'full_address'] = row.address
            else:
                print(f"==>No assessment number for {row.address} | an: {an}")

        elif assn_nr_list[0] == '0' and row.address != '0' and row.address != 0:
            # No assessment number to identify the property
            # is there a good match based only on the address?
            lv_addr_match = lv[lv.address == row.address]
            if len(lv_addr_match) >= 1:
                # lucky match!

                addresses_and_buildings = [f"{bu}, {ad}" for ad,bu in zip(lv_an_match.address, lv_an_match.building_name)]
                full_address = "\n".join(addresses_and_buildings)
                new_assn_nrs = [an_an for an_an in lv_addr_match.assessment_number.values]
                new_arvs = [x for x in lv_addr_match.arv.values]
                
                df.loc[k, 'full_address'] = full_address
                df.loc[k, 'assessment_number'] = str(new_assn_nrs)
                df.loc[k, 'arv'] = str(new_arvs)
                df.loc[k, 'combined_arv'] = lv_addr_match.arv.values.sum()

            elif len(lv_addr_match) == 0:
                # the address is not present verbatim
                # in the landvaluation database.
                # can we do a more subtle match?
                lv_partial_addr_match = lv[lv.address.str.contains(row.address, regex=False)]
                if len(lv_partial_addr_match) >= 1:
                    addresses_and_buildings = [f"{bu}, {ad}" for ad,bu in zip(lv_an_match.address, lv_an_match.building_name)]
                    full_address = "\n".join(addresses_and_buildings)
                    new_assn_nrs = [an_an for an_an in lv_addr_match.assessment_number.values]
                    new_arvs = [x for x in lv_addr_match.arv.values]
                    
                    df.loc[k, 'full_address'] = full_address
                    df.loc[k, 'assessment_number'] = str(new_assn_nrs)
                    df.loc[k, 'arv'] = str(new_arvs)
                    df.loc[k, 'combined_arv'] = lv_addr_match.arv.values.sum()
                else:
                    # no partial address match with "contains"
                    # can we try to match with the begginning of the address?
                    # remove address after last comma
                    addr_begining = ",".join(row.address.split(',')[:-1])
                    if len(addr_begining) > 10:
                        # check that "something" is left after removing the last comma
                        lv_partial_addr_match = lv[lv.address.str.contains(addr_begining, regex=False)]
                        if len(lv_partial_addr_match) == 1:
                            # single match. Very likely to be right
                            final_filter = lv_partial_addr_match[lv_partial_addr_match.parish == row.parish]
                            if len(final_filter) == 1:
                                df.loc[k, 'full_address'] = final_filter.address.values[0]
                                df.loc[k, 'assessment_number'] = final_filter.assessment_number.values[0]
                                df.loc[k, 'arv'] = final_filter.arv.values[0]
                                df.loc[k, 'combined_arv'] = final_filter.arv.values[0]
                        else:
                            # several properties from landvaluation match the address
                            # but we don't have more information to make a decision.
                            final_filter = lv_partial_addr_match[lv_partial_addr_match.parish == row.parish]
                            if len(final_filter) == 1 and row.parish != '0':
                                df.loc[k, 'full_address'] = final_filter.address.values[0]
                                df.loc[k, 'assessment_number'] = final_filter.assessment_number.values[0]
                                df.loc[k, 'arv'] = final_filter.arv.values[0]
                                df.loc[k, 'combined_arv'] = final_filter.arv.values[0]
                    else:
                        # the address had no commas or was formatted 
                        # in a way that resulted in no matches.
                        # we also don't have assessment numbers to help us.
                        # so we keep the address as is.
                        if len(row.address) > 1:
                            df.loc[k, 'full_address'] = row.address
                        else:
                            df.loc[k, 'full_address'] = 'Unknown'
        else:
            # an is 0 and address is usually 0.
            # we ignore these entries which will be deleted as ghost assessment numbers
            pass
    return df

def remove_close_duplicate_sales(df):
    """
    This function removes duplicates with a difference in registration date of less than 4 months
    Sometimes LTRO seems to record the same sale twice with different registration dates and 
    registration numbers.  Here we remove the duplicates with a difference in registration date of less than 4 months.
    as long as the price, address and assessment number are the same for the two records.
    - we also remove duplicates with the SAME application_number and registration_date.
    :param df: dataframe with sales data
    :return: dataframe with duplicates removed
    """

    # Create a copy of the dataframe with assessment_number converted to string
    df_check = df.copy()
    df_check['assessment_number'] = df_check['assessment_number'].astype(str)
    # Find duplicates based on assessment_number, price and full_address
    duplicates = df_check[df_check.duplicated(subset=['assessment_number', 'price', 'full_address'], keep=False)]
    # Sort duplicates by assessment_number to group them together
    duplicates = duplicates.sort_values('assessment_number')
    # delete duplicates with a difference in registration date of less than 4 months
    duplicates['registration_date'] = pd.to_datetime(duplicates['registration_date'])
    duplicate_to_delete = []
    for i in range(len(duplicates)):
        if i % 2 != 0:
            if abs((duplicates.iloc[i].registration_date - duplicates.iloc[i-1].registration_date).days / 30.44)< 4:
                # verify that consecutive lines have the same assessment number:
                if duplicates.iloc[i].assessment_number == duplicates.iloc[i-1].assessment_number:
                    duplicate_to_delete.append(duplicates.iloc[i].name)
    df.drop(duplicate_to_delete, inplace=True)

    # remove one of the duplicates with the SAME application_number and registration_date.
    # potential improvement: keep the one with the most data.
    df = df[~df.duplicated(subset=['application_number', 'registration_date'], keep='first')]

    # remove duplicates with the SAME application_number and registration_date.
    # potential improvement: keep the one with the most data.
    df = df[~df.duplicated(subset=['application_number', 'registration_date'], keep='first')]
    


    return df

