# process property skipper data

import configparser
import utils.skipperutils as skipu

# Get secret URL API
keys = configparser.ConfigParser()
keys.read("./utils/kw_config.txt")
url = keys.get("skipper", "URL")

# download XML and convert to CSV
csv_data = skipu.download_skipper_xml(url)
print("\n Last XML downloaded and saved to ./data/skipper/ \n")
# note that we neeed a strategy to clear up old
# CSV files as they will start taking up space (~1.5MB each)

# flag properties with bad address, assessment number, country
df = skipu.clean_and_flag_properties(csv_data)


# make property type uniform
df = skipu.uniform_property_type(df)

# add land or fractional to property_type if appropriate
df = skipu.identify_land_and_fractional(df)
print("properties cleaned, property_type identified. FINISHED.\n\n")

# to do: 
# - identify land
# - reverse order (clean and flag, after idenfity land and fractional which are not flagged)
 

