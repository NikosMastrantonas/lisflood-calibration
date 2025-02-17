#!/usr/bin/env python3
import os
import argparse
import pandas as pd
from datetime import datetime

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('settings_file', help='Calibration settings file')
    args = parser.parse_args()

    settings_file = args.settings_file
    
    # get the main directory from where inputs and outputs will be read/saved
    main_dir = settings_file.replace('/data/templates/settings.txt', '')
    
    # read stations metadata and keep columns of interest
    metadata_stations = os.path.join(main_dir, 'data/stations/stations_data.csv')
    metadata_stations = pd.read_csv(metadata_stations)
    columns_kept = ['ObsID', 'StationName', 'River', 'Catchment', 'EC_Catchments',
                    'DrainingArea.km2.Provider', 'StationLon', 'StationLat', 'Height',
                    'DrainingArea.km2.LDD5k', 'LisfloodX5k', 'LisfloodY5k', 
                    'DrainingArea.km2.LDD', 'LisfloodX', 'LisfloodY',
                   ]
    columns_kept = list(set(columns_kept)&set(metadata_stations.columns))
    metadata_stations = metadata_stations[columns_kept]

    # read calibration results for each catchment
    print('Reading pHistoryWRanks.csv files for each catchment...')
    calibration_data = []
    for i_catch in metadata_stations.ObsID:
        try:
            i_data = pd.read_csv(os.path.join(main_dir, 'catchments', str(i_catch), 'pHistoryWRanks.csv')).iloc[[0]]
            i_data.index = [i_catch]
            calibration_data.append(i_data)
        except:
            print(f'No pHistoryWRanks.csv file for catchment {i_catch}!')
    
    # combine all results
    calibration_data = pd.concat(calibration_data, axis=0)

    # merge with the metadata and save final sv file
    data_all = pd.merge(left=metadata_stations, right=calibration_data, left_on='ObsID', right_index=True, how='outer')
    data_all.to_csv(os.path.join(main_dir, 'summary/calibration_summary.csv'))

    print("==================== END ====================")