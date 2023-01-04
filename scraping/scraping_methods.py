# functions for landvaluation scraping

import os
import time
import glob
from datetime import datetime

from splinter import Browser
from bs4 import BeautifulSoup
import pandas as pd


def init_browser(url):
    mycwd = os.getcwd()
    executable_path = {'executable_path' : mycwd + '/chromedriver_108'}
    # print(executable_path)
    # I did chmod +x the driver, works for version 106 of chrome
    # make sure to pip install splinter[selenium4]
    # so that selenium can drive the browser
    browser = Browser('chrome', **executable_path)
    
    # Open Site
    browser.visit(url)
    browser.driver.maximize_window()  # full screen to view all menus
    return browser

def get_parish_data(browser):
    parish_list = browser.find_by_id('ContentPlaceHolder1_ddlParish').text.split('\n')
    # value from 1: "City of Hamilton" to 11: "Warwick"
    for i in range(1, 12):
        parish = parish_list[i].strip()
        # for each number
        browser.find_by_id('ContentPlaceHolder1_ddlParish').select(str(i))
        time.sleep(0.5)
        browser.find_by_id('ContentPlaceHolder1_btnSearch').click()
    
        time.sleep(2)
    
        while browser.is_element_visible_by_css('img[src="Assets/images/search_ani2.gif"]'):
            time.sleep(5)
            print('waiting for ' + parish)
    
        html = browser.find_by_id('ContentPlaceHolder1_gvAssessmentList')[0].html

        parish_table_file ="./tmp_data/{}.html".format(parish)
        with open(parish_table_file, "w") as f:
            print('--> saving ' + parish + '\n')
            f.write(html)

    browser.quit()


def process_landval_data():
    # The dataframe where we will save all the data
    result = pd.DataFrame(columns=['assn_nr', 'arv', 'tax_code', 'property_type', 'address', 'grid', 'parish', 'building_name'])

    # what is the position of each piece of information
    # on the table with the results?
    landval_info_position = {0: 'assn_nr', 1:'arv', 2: 'Historic_ARVs', 3: 'tax_code', 4: 'property_type', 5: 'building_name', 6: 'address', 7: 'grid'}

    tmp_dir = "./tmp_data/"
    directory_list = os.listdir(tmp_dir)

    today = datetime.today()
    outfile = '{}-{}-{}_landvaluation_data.csv'.format(today.year, today.month, today.day)

    nr_processed_so_far = 0
    for parish_table in directory_list:
        soup = BeautifulSoup(open(tmp_dir + parish_table,
                                  encoding="utf8"),
                             features="html.parser")
        extracted = soup.find_all("tr", attrs={'style': 'background-color:White;'})
        which_parish = parish_table.split(".html")[0]
        
        print("processing {} {} properties. - {} done. ".format(len(extracted), which_parish, nr_processed_so_far))
        for k, row in enumerate(extracted):
            rowvals = row.find_all('td')
            landval_dict = {}
            landval_dict['parish'] = which_parish
            # note this assumes files name as: Parish.html when scraping
        
            for j, rowval in enumerate(rowvals[1:]):
                landval_dict[landval_info_position[j]] = rowval.text
                # add a new row after the last line
            result.loc[len(result)] = landval_dict
            # append results to CSV
        result.to_csv(outfile, index=False, encoding="utf-8-sig")
        nr_processed_so_far += len(extracted)

    # delete temp html files
    files = glob.glob(tmp_dir + "*")
    for f in files:
        os.remove(f)
