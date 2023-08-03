# functions for cleaning up LTRO data
import decimal
from operator import itemgetter

import pandas as pd
import numpy as np

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
        
    # merge the two headers which are weirdly split over two rows
    # https://stackoverflow.com/q/44799264/
    merged_header = df.loc[0].combine_first(df.loc[1])
    
    # turn that into a list
    header_list = merged_header.values.tolist()
    
    # hack for 2022 as the sheet has yet again different headers
    new_header_list = ['application_number', 'kill', 'sale_type',
                       'kill', 'registration_date', 
                       'kill', 'kill', 'parish', 'kill', 'parcel_area', 'kill',
                       'assessment_number_list', 'address', 'kill',
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
    
    df = df.drop(df[(df.address.str.len() < 5) & (df.assessment_number_list == 0)].index)
    df = df.drop(df[(df.address.str.len() < 5) & (df.assessment_number_list == "Unknown")].index)
    # - We will preserve addresses with short code like SM-800/1, DE-1886/A, *, etc
    # matching those addresses to assessment numbers or addresses is done by function
    # clean_parcel_id_based_addresses() below
    
    # Remove time from the timestamps
    df['registration_date'] =  pd.to_datetime(df['registration_date'], format='%Y-%m-%d %H:%M:%S.%f').dt.date
    # experimental:
    df.reset_index(drop=True, inplace=True)

    return df

def get_assessment_number(assn_nr):
    '''
    Reads the element in the dataframe containing
    the assessment number.  This can have integers,
    non-sensical strings, descriptors (like "Dock" or "Land").
    If it contains a valid assessment number or a comma separated
    list of assessment numbers it will return them.  Otherwise, it
    will return False.
    '''
    assn_nr = str(assn_nr).strip()
    if len(assn_nr) < 7:
        return False
    if (len(assn_nr) == 7) or (len(assn_nr) == 8) or (len(assn_nr) == 9):
        try:
            int(assn_nr)
            return [assn_nr]
        except:
            # it was not a number
            return False
    if len(assn_nr) > 9:
        # it may be a list of assessment numbers
        # remove "&" or "and" and other symbols from the lists
        assn_nr = assn_nr.replace(", and", ",")
        assn_nr = assn_nr.replace(",and", ",")
        assn_nr = assn_nr.replace("&", ",")
        assn_nr = assn_nr.replace("[", "")
        assn_nr = assn_nr.replace("]", "")
        assn_nr = assn_nr.replace("'","") 
        list_of_ass_nr = assn_nr.split(",")
        if len(list_of_ass_nr) <= 1:
            # not a list of assessment numbers
            return False
        else:
            # process list of assessment numbers
            # remove empty space
            clean_list = [x.strip() for x in list_of_ass_nr]
            # recursively check for assessment numbers (number, length, etc)
            assn_nr_clean = [get_assessment_number(x) for x in clean_list]
            # keep list elements that are not false
            an_list = [x[0] for x in assn_nr_clean if x]
            if len(an_list) == 0:
                # did we end up with an empty list?
                return False
            else:
                return an_list


def identify_fractionals(df):
    '''
    identify which rows correspond to sales of fractional properties.
    These will contain certain keywords and have no assessment number
    input: dataframe (note: must already contain column "property_type")
    output: dataframe
    '''
    usual_fractions = [f"1/{n} th" for n in range(2,21)]+[f"1/{n}th" for n in range(2,21)]
    fractional_keywords = ["fraction", "frational", "factional", "fractional", "1/10 share", 
                       "th share", "one tenth", "one sixth", "timeshares", "timeshare",
                      "081265514", "081248016", "071919105", "1/10 fraction", "1/10 fractionof"] 
                    # last 3 numbers are the assess_nr of compounds with lots of apartments
                    # like Newstead Belmont Hills, the Reefs, Harbour Court, Tucker's Point
    fractional_keywords = fractional_keywords + usual_fractions
    
    # not terribly efficient, but ok for small dataset
    property_type_list = []
    
    for index, row in df.iterrows():
        match_assn = any(x for x in fractional_keywords if x in str(row["assessment_number_list"]).lower())
        match_addr = any(x for x in fractional_keywords if x in str(row["address"]).lower())
        match_nintr = any(x for x in fractional_keywords if x in str(row["Nature of\nInterest"]).lower())    
  
        if match_assn or match_addr or match_nintr:
            # some properties may be mis-identified
            # if they have assessment number different from the above
            # but contain fraction keywords.  Optimization for another day.
            if "vacant lot" not in str(row.assessment_number_list).lower():
                # catch lands which are not fractionals
                property_type_list.append("fractional")
            else:
                property_type_list.append(0)
        else:
            property_type_list.append(0)
    df['property_type'] = property_type_list
    return df



        
def identify_land_house_condo(df):
    '''
    identify which rows correspond to sales of lands which
    will contain certain keywords and have no assessment number
    input: dataframe (note: must already contain column "property_type")
    output: dataframe
    '''
    land_keywords = ["vacant lot", "lot of land", "land on", "lot", "land lying", 
                 "land situate", "land situated", "share in land", "government land"]
    land_anti_keywords = ["fairyland lane", "fruitland lane", "camelot", "jiblot", "treslot", "3 scenic lane"] # not lands

    # not terribly efficient, but ok for small dataset
    for index, row in df.iterrows():
        assn_nr = str(row['assessment_number_list']).lower()
        addr = str(row['address']).lower()
        mode = str(row['Mode of\nAcquisition']).lower()
        nature = str(row['Nature of\nInterest']).lower()
        
        match_assn = any(x for x in land_keywords if x in assn_nr)
        match_addr = any(x for x in land_keywords if x in addr)
        
        
        if row["property_type"] == 0:
            # not a fractional property
            an = get_assessment_number(row["assessment_number_list"])
            if (match_assn or match_addr):
                # has land keywords
                if not an: # has no assessment nr (thus probably land)
                    # update property type
                    df.loc[index,'property_type'] = 'land'
            elif ('leaseholder' in nature) and (("lower" in addr) or ("apartment" in addr) or ("unit" in addr)):
                    df.loc[index,'property_type'] = 'condo'
            elif ('condos' in addr) and ('unit' in addr):
                if an: # has assessment number
                    df.loc[index,'property_type'] = 'condo'
            elif ('conveyance' in mode or 'coveyance' in mode) and ('lease' not in mode):
                    df.loc[index,'property_type'] = 'house'
            elif ('lease' in mode or 'leashold' in mode) and 'conveyance' not in mode:    
                    df.loc[index,'property_type'] = 'condo'
            elif ('lease' in mode) and an:
                # one of the LTRO items has a contradiction
                # containing both 'lease' and 'conveyance'
                # assume condo
                list_of_property_types.append('condo')
    
    return df


def process_duplicates(df):
    
    duplis = df[df['application_number'].duplicated(keep=False)]
    current_row = duplis[["application_number","assessment_number_list", "price"]].iloc[0].tolist()
    processed_rows = []
    marked_for_delete = []

    for index, row in duplis.iterrows():

        current_row = [row.application_number, row.assessment_number_list, row.price]
        duplis_subset = duplis[duplis["application_number"] == row.application_number]
        an = [get_assessment_number(x) for x in duplis_subset['assessment_number_list']]
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
                df.loc[dupli_indx,'assessment_number_list'] = final_assn_nr
                df.loc[dupli_indx,'address'] = final_addr

                # mark duplicates for deletion
                marked_for_delete.extend(dupli_indx[1:])

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
                    unknowns = duplis_subset.assessment_number_list.tolist()
                    unknowns = list(map(lambda x: str(x).lower(), unknowns))
                    # in which position of our diplicates subset is the unkown assessment number?
                    if 'unknown' in unknowns:
                        print('\nHERE TOO', unknowns)
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
        an = get_assessment_number(row.assessment_number_list)
        if an and len(an) == 1: # single assessment number
            try:
                arv = lv[lv.assn_nr == int(an[0])].arv.values
            except:
                arv = [0]
            # keep arvs (should be just 1) where something was found
            if len(arv) == 1: 
                arvs_for_ltro.extend(arv)
            elif len(arvs) >=1: 
                # this shouldn't happen (more than 1 ARV for a single Ass. Nr.)
                # but just in case
                arvs_for_ltro.append(arv)
            else: # no ARV found
                arvs_for_ltro.append(0)
        elif an and len(an) > 1: # Multiple assessment numbers
            # get ARVs from landvaluation that match those in LTRO
            arvs = [lv[lv.assn_nr == int(x)].arv for x in an]
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
    `input`: Dataframes from LTRO (df) and from Landvaluation (lv)
    `output`: processed LTRO sales dataframe (df)
    '''
    for index, row in df.iterrows():
        an = get_assessment_number(row.assessment_number_list)
        if an and len(an) == 1:
            try:
                p_type = lv[lv.assn_nr == int(an[0])].property_type.values
                if p_type.size == 0:
                    p_type == False
            except:
                p_type = False
            if p_type.size > 0:
                if (p_type[0] != row.property_type):
                    # trust the value from Land Valuation
                    df.loc[index,'property_type'] = p_type[0]
        elif an and len(an) > 1: # Multiple assessment numbers
            # get property_types from landvaluation that match those in LTRO
            p_types = [lv[lv.assn_nr == int(x)].property_type for x in an]
            # get the value from the dataframe (if it was found, i.e. len(x) > 0)
            p_types = [x.values[0] for x in p_types if len(x) > 0]
            p_types = list(set(p_types)) # keep unique values
            # we will only resolve straightforward issues
            # that is when a single denomination exists for multiple properties
            # for example: They are all apartments, or all houses, or all commercial
            if len(p_types) == 1:
                if row.property_type != p_types[0]:
                    # trust the value from Land Valuation
                    df.loc[index,'property_type'] = p_types[0]
            elif row.property_type == 0 and len(p_types) > 0:
                # if there's nothing, keep the first one from land valuation
                df.loc[index, 'property_type'] = p_types[0]
            elif len(p_types) == 0:
                # no matching assessment numbers found in land valuation
                # thus the p_types list is empty.
                df.loc[index,'property_type'] = False

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
    df["parish"] = df.parish.replace(["Town of St. George", "City of Hamilton"], 
                                 ["St. George's", "Pembroke"], regex=False)
    return df
        

def clean_parcel_id_based_addresses(df):
    """
    Attempts to identify properties which have an address
    less than 10 char long based on Norwood dataset
    for example will match: PE-3121 => 3 Jane Doe Lane
    """
    dfclean = df.copy(deep=False)
    nw = pd.read_csv(NORWOOD_DATA_PATH + "parcel_id_assn_nr_database.csv")
    lv = pd.read_csv(DATA_PATH + "kw-properties.csv")

    addr_matches = []
    addr_n_assn_nr_matches = []
    no_match = []

    
    for df_index, row in dfclean.iterrows():
        assn_str = str(row.assessment_number_list)
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
                assn_list = [int(x) for x in assn_strs]
                    # find it in the land valuation (lv dataframe)
                addresses = ""
                for j, assn_x in enumerate(assn_list):
                    addresses += lv[lv.assn_nr == assn_x]["building_name"].values[0]
                    addresses += ", " + lv[lv.assn_nr == assn_x]["address"].values[0]
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
                assn_nr_match = nw[nw.parcel_id == row.address].assn_nr
                dfclean.loc[df_index, 'assessment_number_list'] = assn_nr_match.iat[0]
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
