# process property skipper data

import configparser
import utils.skipperutils as sku

# Get secret URL API
keys = configparser.ConfigParser()
keys.read("./utils/kw_config.txt")
url = keys.get("skipper", "URL")

# download XML and convert to CSV
csv_data = sku.download_skipper_xml(url)

# note that we neeed a strategy to clear up old
# CSV files as they will start taking up space (~1.5MB each)

clean_and_flag_properties(csv_data)

