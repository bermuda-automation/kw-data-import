import hashlib
import re
import subprocess

from thefuzz import fuzz
from pyproj import CRS, Transformer

from dateutil.parser import parse

import pandas as pd
import xml.etree.ElementTree as ET


def get_xml_with_wget(url, output_file):
    try:
        subprocess.run(["wget", "-O", output_file, url], check=True)
        print(f"Successfully downloaded to {output_file}")
        with open(output_file, 'r') as f:
            return f.read()
    except subprocess.CalledProcessError as e:
        print(f"Error downloading: {e}")
        return None


def transaction_xml_to_dataframe(xml_file):
    """
    - opens the XML previously downloaded from skipperstats
    - loops over the transactions extracting fields of interest
    - exports list of fields to dataframe
    :param xml_file: path to XML file
    :return: dataframe
    """
    tree = ET.parse(xml_file)
    # get root element
    root = tree.getroot()
    # make list of transactions with fields of interest
    # by looping over all transactions.
    all_sales = []
    for transaction in root.findall("./transactions/transaction"):
        sale = {}
        sale['id'] = transaction.find('id').text
        sale['status'] = transaction.find('status').text
        sale['transaction_date'] = transaction.find('transaction_date').text
        # convert price with commas to int
        if transaction.find('price').text is not None:
            sale['price'] = float(transaction.find('price').text.replace(',', ''))
        else:
            sale['price'] = None 
        sale['sold_to_international_purchaser'] = transaction.find('sold_to_international_purchaser').text
        sale['comment'] = transaction.find('comment').text
        sale['ref'] = transaction.find('listing/ref').text if transaction.find('listing/ref') is not None else None
        sale['skipper_id'] = transaction.find('listing').attrib['id']
        sale['property_type'] = transaction.find('listing/property_type').text if transaction.find('listing/property_type') is not None else None
        sale['photos'] = []
        for photo in transaction.findall('listing/photos/photo/path'):
            sale['photos'].append(photo.text)
        sale['assessment_number'] = transaction.find('listing/assessment/assessment_number').text if transaction.find('listing/assessment/assessment_number') is not None else None
        sale['address_line'] = transaction.find('listing/assessment/address_line').text if transaction.find('listing/assessment/address_line') is not None else None
        sale['building_name'] = transaction.find('listing/assessment/building_name').text if transaction.find('listing/assessment/building_name') is not None else None
        sale['parish'] = transaction.find('listing/assessment/parish').text if transaction.find('listing/assessment/parish') is not None else None
        sale['postcode'] = transaction.find('listing/assessment/postcode').text if transaction.find('listing/assessment/postcode') is not None else None
        sale['latitude'] = transaction.find('listing/assessment/latitude').text if transaction.find('listing/assessment/latitude') is not None else None
        sale['longitude'] = transaction.find('listing/assessment/longitude').text if transaction.find('listing/assessment/longitude') is not None else None
        sale['arv_default'] = transaction.find('listing/assessment/arv_default').text if transaction.find('listing/assessment/arv_default') is not None else None
        sale['is_land'] = transaction.find('listing/assessment/is_land').text if transaction.find('listing/assessment/is_land') is not None else None
        sale['is_fractional_unit'] = transaction.find('listing/assessment/is_fractional_unit').text if transaction.find('listing/assessment/is_fractional_unit') is not None else None
        all_sales.append(sale)
    df = pd.DataFrame(all_sales)
    return df

def in_keywords(x, keywords):
    '''
    Check if any of the keywords is in the string x
    '''
    if pd.isnull(x):
        return False
    if x == 0:
        return False
    else:   
        return any([keyword in x for keyword in keywords])

def identify_fractionals(df):
    '''
    identify which rows correspond to sales of fractional properties.
    These will contain certain keywords and have no assessment number
    input: dataframe (note: must already contain column "property_type")
    output: dataframe
    '''

    # change df.property_type to 'fractional' if is_fractional_unit == 1
    df.loc[df.is_fractional_unit == '1', 'property_type'] = 'fractional'

    usual_fractions = [f"1/{n} th" for n in range(2,21)]+[f"1/{n}th" for n in range(2,21)]
    fractional_keywords = ["fraction", "frational", "factional", "fractional", "1/10 share", 
                       "th share", "one tenth", "one sixth", "timeshares", "timeshare",
                      "081265514", "081248016", "071919105", "1/10 fraction", "1/10 fractionof"] 
                    # last 3 numbers are the assess_nr of compounds with lots of apartments
                    # like Newstead Belmont Hills, the Reefs, Harbour Court, Tucker's Point
    fractional_keywords = fractional_keywords + usual_fractions
    
    building_condition = df['building_name'].str.lower().apply(lambda x: in_keywords(x, fractional_keywords))
    ass_nr_condition = df['assessment_number'].str.lower().apply(lambda x: in_keywords(x, fractional_keywords))
    addr_condition =  df['address_line'].str.lower().apply(lambda x: in_keywords(x, fractional_keywords))
    type_condition =  df['property_type'].str.lower().apply(lambda x: in_keywords(x, fractional_keywords))

    df['new_fractionals'] = ( building_condition | ass_nr_condition | addr_condition | type_condition )
    # if it's found to be fractional then make sure the property_type is fractional
    df.loc[df.new_fractionals == True, 'property_type'] = 'fractional'
    # if it's found to be fractional then make sure the assessment_number is None.
    # since fractional properties don't have assessment numbers
    df.loc[df.new_fractionals == True, 'assessment_number'] = None

    df = df.drop('new_fractionals', axis = 1)
    print(' -> identifying fractionals')
    return df


def identify_lands(df):  

    # change df.property_type if is_land == 1
    df.loc[df.is_land == '1', 'property_type'] = 'land'

    land_keywords = ["vacant lot", "lot of land", "land on", "lot", "land lying", 
                 "land situate", "land situated", "share in land", "government land"]
    land_anti_keywords = ["fairyland lane", "fruitland lane", "camelot", "jiblot", "treslot", "3 scenic lane"] # not lands

    df['address_line'] = df['address_line'].fillna('')
    df['building_name'] = df['building_name'].fillna('')

    # Creating conditions for both the keywords and anti-keywords
    keyword_conditions = df['address_line'].str.lower().apply(lambda x: in_keywords(x, land_keywords))
    keyword_conditions_building = df['building_name'].str.lower().apply(lambda x: in_keywords(x, land_keywords))

    anti_keyword_conditions = df['address_line'].str.lower().apply(lambda x: not in_keywords(x, land_anti_keywords))
    anti_keyword_conditions_building = df['building_name'].str.lower().apply(lambda x: not in_keywords(x, land_anti_keywords))

    # Applying both conditions
    df['new_lands'] = (keyword_conditions | keyword_conditions_building)  & (anti_keyword_conditions & anti_keyword_conditions_building)
    df.loc[df.new_lands == True, 'property_type'] = 'land'
    # if it's found to be a land then make sure the assessment_number is None.
    # since lands don't have assessment numbers
    df.loc[df.new_lands == True, 'assessment_number'] = None

    df = df.drop('new_lands', axis=1)
    print(' -> identifying lands')

    return df


def application_number_hash(a,b,c,d):
    """Takes 4 values associated with a property 
    and returns a hash of them"""
    code_hash = hashlib.md5( str(a).encode('utf-8') + str(b).encode('utf-8') + str(c).encode('utf-8') + str(d).encode('utf-8')).hexdigest()
    return code_hash[0:8]

def drop_unidentified(df):
    """
    Some properties are completely unidentified other than by price and date.
    we fix a few with an adhoc function, which gives some matches
    found by hand.  This function can only be applied after application_number_hash()
    has been applied.
    """
    
    # boolean indexing to select the row to drop
    mask1 = (df['transaction_date'].astype(str).str.contains('2018-02-07')) & \
            (df['application_number'] == 'skip-b30ec9f2')
    mask2 = (df['transaction_date'].astype(str).str.contains('2018-08-24')) & \
            (df['application_number'] == 'skip-a11b61f3')
    # this one has two approximate matches (not sure which one it is, 
    # but we also drop it since we can't identify it.
    mask3 = (df['transaction_date'].astype(str).str.contains('2018-01-25')) & \
            (df['application_number'] == 'skip-b04f112f')
    # drop the selected row using the mask
    print(" -> dropping unidentified sales")
    df = df.drop(df[mask1 | mask2 | mask3].index)

    return df

def drop_selected_duplicates_by_hand(df):
    # adhoc dropping as I found it to be a duplicate
    # but it's hard for it to be identified by the deduplicate functions
    # due to very fuzzy and partial matching of address
    df = df[~((df.application_number == 'skip-5d5b02ba') | \
              (df.application_number == 'skip-362a202c') | \
              (df.application_number == 'skip-2d47fd0e') | \
              (df.application_number == 'skip-dae3cf28') | \
              (df.application_number == 'skip-c1142a29dae3cf28') | \
              (df.application_number == 'skip-793b8b4f') | \
                (df.application_number == 'skip-d95b02ba'))]
    return df

def clean_up_skipperstats_data(df):

    # drop rows with no price
    df = df[~(df.price == 0.0)]


def _fuzzy_address_match(addr1, addr2):
    """
    Compares two address strings using the fuzzywuzzy library.
    and returns a boolean:
    True if the address strings are similar
    False if they are dissimilar enough to be considered different
    :param str addr1: first address string
    :param str addr2: second address string
    :return: bool

    """
    if 'Bermuda' not in addr2:
      addr2 = addr2 + ', Bermuda'
    if 'one tenth' in addr2.lower():
        addr2 = addr2.replace('One Tenth', '1/10th')
    if 'one sixth' in addr2.lower():
        addr2 = addr2.replace('One Sixth', '1/6th')
    if 'Bermuda' not in addr1:
      addr1 = addr1 + ', Bermuda'

    numbers_only_0 = set(re.findall('\d+', addr1))
    numbers_only_1 = set(re.findall('\d+', addr2))
    if len(numbers_only_0) == 0 or len(numbers_only_1) == 0:
        # avoid division by zero
        return False, 0
    else:
        number_match_ratio = len(numbers_only_0.intersection(numbers_only_1))/len(numbers_only_0)

    similarity_ratio = fuzz.ratio(addr1, addr2)

    if similarity_ratio > 80 and (numbers_only_0 == numbers_only_1):
        return True, similarity_ratio
    elif similarity_ratio > 80 and number_match_ratio >= 0.5:
        # addresses are very similar and more than half the numbers match
        # (typically because a postcode is missing)
        return True, similarity_ratio
    elif similarity_ratio <= 80 and (numbers_only_0 == numbers_only_1):
        # although the first characters don't coincide,
        # they are close enough we can remove the first one
        trunc_similarity_ratio = fuzz.ratio(addr1[15:], addr2[15:])
        if trunc_similarity_ratio > 60:
              return True, trunc_similarity_ratio
        else:
            # Is there's a partial match for "end of address" 
            # and "numbers" in address actually match?
            end_similarity_ratio = fuzz.ratio(addr1[-30:], addr2[-30:])
            if end_similarity_ratio > 60:
                return True, end_similarity_ratio
            else:
                return False, end_similarity_ratio
    else:
        return False, similarity_ratio


def parse_mixed_dates(date_str):
    try:
        return parse(date_str, dayfirst=False, yearfirst=False)
    except (ValueError, TypeError):
        # If dateutil fails, try custom parsing
        if pd.isna(date_str) or date_str == '0':
            return pd.NaT
        if 'jUNE' in date_str:
            date_str = date_str.replace('jUNE', 'Jun')
            return parse(date_str, dayfirst=True, yearfirst=False)
        else:
            return pd.NaT


def date_filter_for_sss_LTRO_duplicates(df, sa):
    """ SSS (Skipper Stats Sales) | LTRO (Land Title Registry)
    this function uses the LTRO sales dataframe 
    to filter out sales from the skipperstats dataframe which are duplicates
    The strategy is to try to match first based on transaction dates
    :param df: skipperstats dataframe
    :param sa: LTRO sales dataframe
    :return: skipperstats dataframe
    """

    # Make sure date are in comparable formats
    sa['registration_date'] = pd.to_datetime(sa['registration_date'])
    sa['acquisition_date'] = sa['acquisition_date'].apply(parse_mixed_dates)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # Make sure assessment numbers are in comparable formats
    df.assessment_number = df.assessment_number.fillna(0)
    df.assessment_number = df.assessment_number.astype(str)

    single_match = 0
    multi_match = 0
    indexes_of_matches_to_delete = []

    for k, row in df.iterrows():
        # Can we find this row in the sa dataframe?
        ssdate = row.transaction_date
        # print(ssdate, sa.registration_date.dt.day)
        date_match = sa[(sa.registration_date == ssdate) | (sa.acquisition_date == ssdate)]

        # single match
        if date_match.shape[0] == 1:
            # do price and assessment number match too?
            price_match = date_match[date_match.price == row.price]
            if price_match.shape[0] == 1:
                assn_nr_match = price_match[price_match.assessment_number.str.contains(row.assessment_number)]
                if assn_nr_match.shape[0] == 1 and row.assessment_number != '0':
                    # 3 criteria match. We can assume it's a duplicate
                    # we ignore assn_nr = 0 as that provides no useful information to compare
                    indexes_of_matches_to_delete.append(k)
                elif assn_nr_match.shape[0] == 1:
                    # instead of using assn_nr to compare (which is zero in this branch)
                    # use a fuzzy match for the address
                    addr_sss = f"{row.building_name}, {row.address_line}, {row.parish} {row.postcode}, Bermuda"
                    addr_sa = assn_nr_match.address.values[0]
                    addr_match, addr_match_score = _fuzzy_address_match(addr_sss, addr_sa)
                    if addr_match:
                        indexes_of_matches_to_delete.append(k)
                    else:
                        pass
                
            elif price_match.shape[0] == 0:
                # date matches, but not price
                # is there an approximate match for the price?
                # (potentially due to including fees / gross / net price)
                assn_nr_match = date_match[date_match.assessment_number.str.contains(row.assessment_number)]
                almost_price_match = date_match[(date_match.price > row.price * 0.95) & (date_match.price < row.price * 1.05)]
                if (len(almost_price_match) == 1 and len(assn_nr_match) == 1 and row.assessment_number != '0'):
                    indexes_of_matches_to_delete.append(k)
                elif len(almost_price_match) == 1 and len(assn_nr_match) == 1:
                    print("ALMOST PRICE AND DATE MATCH for assessment number = 0")
            else:
                pass # a single match can't have len() > 1

        # multiple date matches
        elif date_match.shape[0] > 1:
            # There are multiples sales from LTRO on that date. Can we filter further?
            price_match = date_match[date_match.price == row.price]
            if price_match.shape[0] == 1:
                # date and price match
                assn_nr_match = price_match[price_match.assessment_number.str.contains(row.assessment_number)]
                if assn_nr_match.shape[0] == 1 and row.assessment_number != '0':
                    # 3 criteria match. We can assume it's a duplicate
                    indexes_of_matches_to_delete.append(k)
                elif assn_nr_match.shape[0] == 1:
                    # date, price match, and assn_nr is zero
                    # match with additional critrion of fuzzy address match
                    addr_sss = f"{row.building_name}, {row.address_line}, {row.parish} {row.postcode}, Bermuda"
                    addr_sa = assn_nr_match.address.values[0]
                    addr_match, addr_match_score = _fuzzy_address_match(addr_sss, addr_sa)
                    if addr_match:
                        indexes_of_matches_to_delete.append(k)
                    else:
                        pass # hard to automate beyond here due to idiosyncracies in the addresses
                        # print(f"WE ARE HERE?: {row.application_number} - ({addr_match}:{addr_match_score}) - {addr_sss} || {addr_sa}")
            elif price_match.shape[0] > 1:
                assn_nr_match = price_match[price_match.assessment_number.str.contains(row.assessment_number)]
                if assn_nr_match.shape[0] == 1  and  row.assessment_number != '0':
                    # several date matches, several price matches, 
                    # but only one property remains when we filter by assn_nr.
                    indexes_of_matches_to_delete.append(k)
                elif assn_nr_match.shape[0] == 1 and row.assessment_number == '0':
                    # apparently not in this dataset
                    pass
                elif assn_nr_match.shape[0] > 1:
                    # not present in this dataset
                    pass
            elif price_match.shape[0] == 0:
                # date matches, but not price
                # is there an approximate match for the price?
                # (potentially due to including fees / gross / net price)
                assn_nr_match = date_match[date_match.assessment_number.str.contains(row.assessment_number)]
                almost_price_match = date_match[(date_match.price > row.price * 0.95) & (date_match.price < row.price * 1.05)]
                if (len(almost_price_match) == 1 and len(assn_nr_match) == 1 and row.assessment_number != '0'):
                    indexes_of_matches_to_delete.append(k)
                elif len(almost_price_match) > 1:
                    # can we filter further?
                    almost_price_and_assn_match = almost_price_match[almost_price_match.assessment_number.str.contains(row.assessment_number)]
                    if len(almost_price_and_assn_match) == 1:
                        indexes_of_matches_to_delete.append(k)
                    elif len(almost_price_and_assn_match) > 1:
                        pass
                    elif len(almost_price_and_assn_match) == 0:
                        pass
            else:
                pass # how would we get here?

        else:
            # not date matches. We don't consider this a duplicate
            pass
    print('\n', len(indexes_of_matches_to_delete), 'Skipper Stats duplicates processed based on dates')
    df = df.drop(indexes_of_matches_to_delete)
    return df

def are_dates_close(skipperdate, ltrodate1, ltrodate2):
    """
    Returns True if the dates are close enough for the sale to be considered the same.
    :param pd.Timestamp skipperdate: row.transaction_date pandas Timestamp from skipperstats
    :param datetime ltrodate1: sa.registration_date datetime from LTRO
    :param datetime ltrodate2: sa.acquisition_date datetime from LTRO
    """
    trans_date = skipperdate  # already pandas Timestamp
    reg_date = pd.Timestamp(ltrodate1) # convert to pandas Timestamp
    acq_date = pd.Timestamp(ltrodate2) # convert to pandas Timestamp
    days_cutoff = 390 # days between dates to be considered the same

    if pd.isna(acq_date) and not pd.isna(reg_date):
        min_date_diff = abs((trans_date - reg_date).days)
    elif not pd.isna(acq_date) and pd.isna(reg_date):
        min_date_diff = abs((trans_date - acq_date).days)
    elif not pd.isna(acq_date) and not pd.isna(reg_date):
        min_date_diff = min(abs((trans_date - acq_date).days), abs((trans_date - reg_date).days))
    elif pd.isna(skipperdate):
        return False # can't compare
    else:
        return False # other problem with dates

    if min_date_diff < days_cutoff and min_date_diff >= 0:
        return True, min_date_diff
    elif min_date_diff > days_cutoff:
        return False, min_date_diff
    else:
        print("NEGATIVE DATE DIFFERENCE!")
        return False, min_date_diff
    
def are_prices_close(skipperprice, ltroprice):
    """
    Returns True if the prices are close enough for the sale to be considered the same.
    """
    price_diff = abs(skipperprice - ltroprice)
    if skipperprice*0.95 <= ltroprice <= skipperprice*1.05:
        return True, price_diff
    else:
        return False, price_diff
    

def are_addresses_close(skipper_addr, ltro_addr):
    numbers_only_0 = set(re.findall('\d+', skipper_addr))
    numbers_only_1 = set(re.findall('\d+', ltro_addr))
    similarity_ratio = fuzz.ratio(skipper_addr, ltro_addr)
    if similarity_ratio > 80 and (numbers_only_0 == numbers_only_1):
        return True
    else:
        return False


def address_filter_for_sss_LTRO_duplicates(df, sa):
    """ SSS (Skipper Stats Sales) | LTRO (Land Title Registry)
    this function uses the LTRO sales dataframe 
    to filter out sales from the skipperstats dataframe which are duplicates
    The strategy is to try to match first based on the address
    :param df: skipperstats dataframe
    :param sa: LTRO sales dataframe
    :return: skipperstats dataframe
    """
    indexes_of_matches_to_delete = []

    for k, row in df.iterrows():
        # Are the partial matches for address and building?
        if row.address_line != 0 and row.building_name != 0:
            addr = row.address_line
            build = row.building_name
            sa_addr_match = sa[(sa.address.str.contains(addr, regex=False)) & (sa.address.str.contains(build, regex=False))]
        else:
            sa_addr_match = pd.DataFrame()

        if len(sa_addr_match) == 1:
            # single partial match for address
            trans_date = row.transaction_date # <- pandas Timestamp
            reg_date = pd.Timestamp(sa_addr_match.registration_date.dt.date.values[0])
            acq_date = pd.Timestamp(sa_addr_match.acquisition_date.dt.date.values[0])

            if are_dates_close(trans_date, reg_date, acq_date)[0]:
                if are_prices_close(row.price, sa_addr_match.price.values[0])[0]:
                    indexes_of_matches_to_delete.append(k)
            else:
                if are_prices_close(row.price, sa_addr_match.price.values[0])[1] <= 6000:
                    # dates are far apart, but prices match exactly
                    # suggesting skipperstats may have mi-labeled the date
                    indexes_of_matches_to_delete.append(k)
                else:
                    pass 
                    # address matches, but date and price don't.
                    # it may be the same house sold at different times.
        elif len(sa_addr_match) > 1:
            # can we filter down with date and price?
            # dates within a year, and price within 10%.
            date_price_match = sa_addr_match[((sa_addr_match.registration_date.dt.year == row.transaction_date.year) | \
                                (sa_addr_match.registration_date.dt.year == row.transaction_date.year + 1) | \
                                (sa_addr_match.registration_date.dt.year == row.transaction_date.year - 1) | \
                                (sa_addr_match.acquisition_date.dt.year == row.transaction_date.year) | \
                                (sa_addr_match.acquisition_date.dt.year == row.transaction_date.year + 1) | \
                                (sa_addr_match.acquisition_date.dt.year == row.transaction_date.year - 1)) & \
                                (sa_addr_match.price >= row.price*0.9) & (sa_addr_match.price <= row.price*1.1)]
            if len(date_price_match) == 1:
                indexes_of_matches_to_delete.append(k)
            else:
                pass
                # either not match or multiple matches we can't discriminate

        else:
            # no partial match
            # Can we do a fuzzy match instead?
            # find fuzzy matches for the address_line in the sa DataFrame
            if row.address_line != 0 and row.building_name != 0:
                addr_sss = f"{row.building_name}, {row.address_line}, {row.parish} {row.postcode}"
            elif row.building_name == 0:
                addr_sss = f"{row.address_line}, {row.parish} {row.postcode}"
            else:
                addr_sss = 0
            if addr_sss != 0:
                fuzzy_addr_match = sa[sa.address.apply(lambda x: are_addresses_close(addr_sss, x))]
            else:
                fuzzy_addr_match = pd.DataFrame()
            
            if len(fuzzy_addr_match) == 1:
                trans_date = row.transaction_date
                reg_date = pd.Timestamp(fuzzy_addr_match.registration_date.dt.date.values[0])
                acq_date = pd.Timestamp(fuzzy_addr_match.acquisition_date.dt.date.values[0])
                if are_prices_close(row.price, fuzzy_addr_match.price.values[0]) and are_dates_close(trans_date, reg_date, acq_date)[0]:
                    indexes_of_matches_to_delete.append(k)
    print(len(indexes_of_matches_to_delete), 'Skipper Stats duplicates processed based on address')
    df = df.drop(indexes_of_matches_to_delete)
    return df


def fractional_filter_for_sss_LTRO_duplicates(df, sa):
    indexes_of_matches_to_delete = []
    sss_frac = df[(df.property_type == 'fractional')] 
    sa_frac = sa[sa.property_type == 'fractional']

    for k,row in sss_frac.iterrows():
        addr = row.address_line
        # find numbers and letters that define the fractional
        # property
        # define the regular expression pattern
        pattern = r'(\d+/\d+|\d+[A-Z]|[A-Z]-\d+|\d{3})'
        # apply the regular expression to the string
        numbers_and_letters = re.findall(pattern, row.building_name)
        if len(numbers_and_letters) == 0:
            sa_frac_match = sa_frac[(sa_frac.address.str.contains(addr, regex=False)) & \
                                    (sa_frac.address.str.contains(row.building_name, regex=False))]
        elif len(numbers_and_letters) == 1:    
            sa_frac_match = sa_frac[(sa_frac.address.str.contains(addr, regex=False)) & \
                                    (sa_frac.address.str.contains(numbers_and_letters[0]))]
        elif len(numbers_and_letters) == 2:
            sa_frac_match = sa_frac[(sa_frac.address.str.contains(addr, regex=False)) & \
                                    (sa_frac.address.str.contains(numbers_and_letters[0])) & \
                                    (sa_frac.address.str.contains(numbers_and_letters[1]))]
        else:
            sa_frac_match = pd.DataFrame()
        
        if (len(sa_frac_match) == 1) and (sa_frac_match.price.values[0] == row.price):
                indexes_of_matches_to_delete.append(k)
        elif (len(sa_frac_match) > 1) and (sa_frac_match.price.values[0] >= row.price*0.95) and (sa_frac_match.price.values[0] <= row.price*1.05):
                indexes_of_matches_to_delete.append(k) 
    
    print(len(indexes_of_matches_to_delete), 'Fractional Skipper Stats duplicates processed')
    df = df.drop(indexes_of_matches_to_delete)
    return df    
    

def lng_lat_to_BDA_east_north(lng, lat):

    # define the source and destination coordinate systems
    # EPSG:4326, https://spatialreference.org/ref/epsg/4326/
    src_crs = CRS.from_proj4("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs") 
    # EPSG:3770, https://spatialreference.org/ref/epsg/3770/
    dst_crs = CRS.from_proj4("+proj=tmerc +lat_0=32 +lon_0=-64.75 +k=1 +x_0=550000 +y_0=100000 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")

    # create a transformer object
    transformer = Transformer.from_crs(src_crs, dst_crs)

    # transform the coordinates using the transformer object
    east, north = transformer.transform(lng, lat)
    return round(east), round(north)

def add_bermuda_grid(df):
    """
    Takes a dataframe with longitude and latutude
    columns and adds a column with the eastings and northings
    based on the Bermuda grid.
    The returned dataframe has a new column called "grid" 
    which has a string with eastings and northings separated by a comma.

    :param df: dataframe (must have columns longitude and latitude)
    :return: dataframe 
    """
    grid = []
    for k, row in df.iterrows():
        lng = str(row.longitude)
        lat = str(row.latitude) 
        if len(lng) < 19 and len(lat) < 19:
            # it is not a legitimate lng, lat
            grid.append(f"{0},{0}")
        elif len(lng) == 19 and len(lat) == 19:
            # it is a legitimate lng, lat, hopefully!
            e,n = lng_lat_to_BDA_east_north(lng, lat)
            grid.append(f"{e},{n}")
        else:
            print("What are these strange coordinates? {}, {}".format(lng, lat))
            grid.append(f"{0},{0}")

    df['grid'] = grid
    print(" -> Bermuda grid with 'Northing' and 'Easting' added.\n")
    return df

def fix_no_name_buildings(df, lv):
    """
    Takes buildings with no name and adds a name based on:
    - a match in the landvaluation database
    - the address and building type fields
    """
    mask = (df.building_name == "0") | (df.building_name == "N/A") | (df.building_name.isna())
    # iterate over the rows in sss that don't have a name
    for idx in df[mask].index:
        match = lv[lv.assessment_number == df.loc[idx, 'assessment_number']]
        if len(match) > 0:
            if len(match.building_name.values[0]) > 2:
                # building name found at landvaluation database
                df.loc[idx, 'building_name'] = match.building_name.values[0]
            else:
                # building name not found at landvaluation database
                building_name = df.loc[idx, 'property_type'] + " at " + df.loc[idx, 'address_line'].split(",")[0]
                df.loc[idx, 'building_name'] = building_name

    return df
