# Scripts for data import

## Sources
The data comes from 3 sources:

- landvaluation.bm (updated rarely - every 6 months maybe)
- propertyskipper.com (updated daily)
- LTRO (Land Title Registry) (updated every 2 or 3 months)

## Data Structure
The data from those 3 sources becomes atributes of 4 objects:

- Property
- SkipperProperty
- Listing
- Sale

See [Miro Diagram of the Database Structure](https://miro.com/app/board/uXjVO49bdWA=/)

## Data Preparation Concepts

Importing the data requires some previous steps:

- Opening the source (a `.xlsx` or an API)
- Converting to `.csv`
- Cleaning up the data
- Making data labels, categories or bins homogeneous
- Saving to processed `.csv`

The data is then ready for import into the webapp.

## Data Preparation Scripts

Preparing the data is divided into 3 scripts:

1. `process_landvaluation.py` (to be run approximately every 6 months)
    > input: `latest_landvaluation_data.csv` 
	> output: `kw-properties.csv`
	
2. `process_skipper.py` (to be run daily)
   > input: `Web API`
   > output: `kw-skipper_properties.csv` and `kw-listings.csv`

3. `process_LTRO.py` (to be run every 3 - 6 months)
   > input: `LTRO_2018-2022.csv` and `LTRO_latest.csv`
   > ouput: `kw-sales.csv`


the data in these 4 separate `.csv` files:
- `kw-properties.csv`
- `kw-skipper_properties.csv`
- `kw-listings.csv`
- `kw-sales.csv`

will seed the final database.

# Details of each script:

This notebook has the following structure:

1. Open landvaluation.bm data
   * Transform `property_type` with Albert's dictionary
   * Sanity check (about 32,000 entries, no duplicates, range of ARVs)
   * save to `kw-properties.csv`


2. Call Property Skipper XML data feed
    * Read, Download & Parse XML file
    * Check for well formed assessment numbers
    * Homogenize Property Type
    * Merge `is_let` and `is_rent`.
    * extract `Listing` event information
    * add `FLAG` to properties needing review
    * save to `kw-skipper_properties.csv` and `kw-listings.csv`

3. Load LTRO Sales data from csv

	- Clean up poorly formatted headers
- Redefine some headers to follow the conventional KW Bermuda database names
- Try to identify sales of "fractional", "land", "house" or "condo"
- Delete obvious duplicates (same 'application_number','registration_date', 'acquisition_date', 'assessment_number_list' and 'price')
- Delete empty rows
- Drop properties which can't be identified (They have no assessment number and no address:  The address is just a code like `WA-840,*, PA-1997, PE-448, SM-800/1, DE-1886/A, TS-418, WA-1629`

- Process rows which have the same *application number* & *price* but different *address* or *assessment number*.
    1. If both (or all) duplicates have a separate assessment number, all assessment numbers and associated addresses will be kept.
    2. If one has an assessment number and the other doesn't (for example an empty lot, vacant lot, dock, Unknown, Government land, etc), the second one will be ignored and removed from the database.
    3. If only one of the duplicates is marked as "fractional" then only that one will be kept
    4. If none have assessment numbers, and none are "fractional", then the first with a non-empty description inside the "assessment number" will be kept.

- Associate one (or several) `ARV` value(s) to each property that has one (or more) assessment number(s)
- Create a `Combined ARV` column, for those sales that involve multiple ARVs
- Filter out `prices` and `ARVs` which are non-sensical.  For example, a sale for **\$100,000** of a building with an ARV of **\$1,729,000** (or a sale of a building on an island for **\$21,000** with an ARV of **\$22,200**, etc.  Anything with `ARV x 3 > price` is ignored (Combined_ARV set to 0)
- Improve property_type data: Land valuation data for property_type is more reliable.  To improve LTRO data (less reliable), when land valuation data exists for property type with the same assessment number it is used to correct the property type from LTRO. we will only resolve straightforward issues: that is when a single denomination exists for multiple properties. for example: They are all apartments, or all houses, or all commercial.  And additionally, if the property type is completely missing from LTRO, we will take the first one from landvalation (1st, because several matches may exist if several assessment numbers are associated with the sale)
    * save to `kw-sales.csv`
  
