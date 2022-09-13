import scraping_methods


url = "http://landvaluation.bm/"
browser = scraping_methods.init_browser(url)

# download HTML files for each parish
scraping_methods.get_parish_data(browser)

# open HTML files locally, exctract data
# and save to csv file
# delete tmp HTML files
scraping_methods.process_landval_data()

