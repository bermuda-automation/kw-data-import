# functions to clean up landvaluation data

from thefuzz import fuzz

def process_and_merge_duplicates(df):
    '''
    this function is applied after last_scraped_data
    and latest_lv_data have been concatenated and full duplicates have been removed
    We now want to remove more subtle duplicates, which are duplicates only
    when considering assessment number and address.
    '''

    all_duplicates = df[df.duplicated(subset=['assessment_number', 'address'] )]
    print(all_duplicates.shape[0], "partial duplicates found")

    if len(all_duplicates) == 0:
        # nothing to do here. No duplicates found
        print("No partial duplicates found")
        return df
    
    duplicates_to_delete = []
    for k, adupe in all_duplicates.iterrows():
        # get a small dataframe with the duplicates matching the current row
        matches = df[df.assessment_number == adupe.assessment_number ]
        # save the name of the building which has a partial match
        build_name = matches.building_name_low.values[0]
        # save the index of the row to delete (if they are similar)
        index_to_delete = df.index[(df['building_name_low'] == build_name) & \
                                    (df.assessment_number == adupe.assessment_number)].tolist()
        similarity_ratio = fuzz.ratio(matches.building_name_low.values[0],
                                      matches.building_name_low.values[1])
        # we assume duplicates are only two
        if similarity_ratio > 80:
            # they are close enough we can remove the old one
            if similarity_ratio == 100 and len(index_to_delete) == 2:
                # the building name is the same. Keep only first one (most recent scraped list)
                duplicates_to_delete.extend([index_to_delete[0]])
            else:
                # index to delete only had one match (len(index_to_delete) == 1))
                duplicates_to_delete.extend(index_to_delete)
        else:
            # match is not good enough
            if len(build_name) < 5:
                # first one only has a few characters, so we don't keep it.
                duplicates_to_delete.extend(index_to_delete)
            else:
                # both building names are long but different
                # so we will keep both (combining them by concatenation)
                duplicates_to_delete.extend(index_to_delete)
                new_building_name = df.loc[index_to_delete, 'building_name'].values + \
                                    ' -- ' + df.loc[k, 'building_name']
                new_building_name_low = df.loc[index_to_delete, 'building_name_low'].values + \
                                        ' -- ' + df.loc[k, 'building_name_low']
                df.loc[k, 'building_name'] = new_building_name[0]
                df.loc[k, 'building_name_low'] = new_building_name_low[0]
            
            
    print(len(duplicates_to_delete), "partial duplicates removed")
    df = df.drop(duplicates_to_delete)
    return df
