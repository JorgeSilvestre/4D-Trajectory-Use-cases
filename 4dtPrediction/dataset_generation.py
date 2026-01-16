import paths
import pandas as pd
from common import get_dates_between

def load_data(date_start: str, date_end: str, origin_airports: str = 'LIRF') -> pd.DataFrame:
    """Loads one month of definitive data

    If sampling is provided, final data is loaded from the corresponding folder.
    Assumes that sampled data have been saved into disk before.
    
    Args:
        month: Month of sorted data to be loaded, in format 'YYYYMM'
        dataset: Set of data to be loaded (either 'train', 'test' or 'val')
        airport: ICAO code of an airport. Only trajectories coming from this
            airport will be included (optional)
    """
    file_paths = [y for x in get_dates_between(date_start,date_end) 
                    for y in paths.data_path.glob(f'tray.{x.strftime("%Y-%m-%d")}.parquet')]
    data = pd.concat([pd.read_parquet(x) for x in file_paths])
    print(data.columns)
    if origin_airports and origin_airports != '*':
        data = data[data.aerodromeOfDeparture == origin_airports]

    return data


    data = data.sort_values(by=['ifplId','timestamp'])

    return data

def feature_engineering(data):
    def assign_time_of_day(x):
        """Assigns time of day depending on the hour"""
        if x.hour < 7:
            return 'night'
        elif x.hour < 13:
            return 'morning'
        elif x.hour < 20:
            return 'evening'
        else:
            return 'night'
    
    # Drop columns
    data = data.drop(['wx_string','sky_condition', 'temperature'], axis=1)

    # Add columns
    data['day_of_week']
    data['hav_distance']
    data['time_of_day']

    # Add objective

    pass

def train_encoder_scaler():
    pass

def main():
    print(load_data('2023-07-03', '2023-07-05'))

if __name__ == '__main__':
    main()