import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import re
import time

class APILog:
    def __init__(self):
        self.burst_speed = 10  # seconds between individual requests
        self.hourly_limit = 50  # requests per hour
        self.request_time = []

    def enforce_rate_limits(self):
        # Check hourly limit
        if len(self.request_time) >= self.hourly_limit:
            while True:
                elapsed = (datetime.now() - self.request_time[-self.hourly_limit]).total_seconds()
                if elapsed > 3600:
                    break
                print(f"The ninja API is limited to {self.hourly_limit} requests per hour: waiting {5 * round((3600 - elapsed) / 5)} seconds to proceed...", end='\r')
                time.sleep(10)

        # Check burst limit
        if len(self.request_time) > 0:
            elapsed = (datetime.now() - self.request_time[0]).total_seconds()
            if elapsed < self.burst_speed:
                time.sleep(self.burst_speed - elapsed)

        self.request_time.insert(0, datetime.now())

apilog = APILog()

def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            raise ValueError("Date format should be either 'YYYY-MM-DD' or 'DD/MM/YYYY'")

def ninja_build_wind_url(lat, lon, from_date='2014-01-01', to_date='2014-12-31', dataset='merra2', capacity=1, height=60, turbine='Vestas+V80+2000', raw='false', format='csv',local_time='true'):
    from_date = format_date(from_date)
    to_date = format_date(to_date)
    return f'https://www.renewables.ninja/api/data/wind?local_time={local_time}&lat={lat}&lon={lon}&date_from={from_date}&date_to={to_date}&capacity={capacity}&dataset={dataset}&height={height}&turbine={turbine}&raw={raw}&format={format}'

def ninja_build_solar_url(lat, lon, from_date='2014-01-01', to_date='2014-12-31', dataset='merra2', capacity=1, system_loss=0.1, tracking=0, tilt=35, azim=180, raw='false', format='csv',local_time='true'):
    from_date = format_date(from_date)
    to_date = format_date(to_date)
    return f'https://www.renewables.ninja/api/data/pv?local_time={local_time}&lat={lat}&lon={lon}&date_from={from_date}&date_to={to_date}&capacity={capacity}&dataset={dataset}&system_loss={system_loss}&tracking={tracking}&tilt={tilt}&azim={azim}&raw={raw}&format={format}'


def ninja_build_weather_url(lat, lon, from_date='2014-01-01', to_date='2014-12-31',dataset='merra2',var_t2m='true',var_prectotland='true',var_precsnoland='true',var_snomas='true',var_rhoa='true',var_swgdn='true',var_swtdn='true',var_cldtot='true',raw='false', format='csv',local_time='true'):
    from_date = format_date(from_date)
    to_date = format_date(to_date)
    return f'https://www.renewables.ninja/api/data/weather?local_time={local_time}&lat={lat}&lon={lon}&date_from={from_date}&date_to={to_date}&dataset={dataset}&var_t2m={var_t2m}&var_prectotland={var_prectotland}&var_precsnoland={var_precsnoland}&var_snomas={var_snomas}&var_rhoa={var_rhoa}&var_swgdn={var_swgdn}&var_swtdn={var_swtdn}&var_cldtot={var_cldtot}&raw={raw}&format={format}'

def ninja_get_url(url):
    apilog.enforce_rate_limits()
    headers = {'Authorization': 'Token add your token here'}
    response = requests.get(url, headers=headers)
    #print(response.text)
    #print(response)
    if response.text[4:19] == "<!DOCTYPE html>":
        pattern = re.compile(
            r'# Renewables\.ninja Weather.*?'
            r'(time,t2m.*?\n.*?\d{4}-\d{2}-\d{2} 23:\d{2},.*?)(?=</pre>)',
            re.DOTALL)
        match = pattern.search(response.text)
        #print(match)
        if match:
            extracted_text = match.group(0)
    else:
        extracted_text=response.text

    response.raise_for_status()
    # if
    data = pd.read_csv(StringIO(extracted_text), skiprows=3)
    data.iloc[:, 0] = pd.to_datetime(data.iloc[:, 0], format='%Y-%m-%d %H:%M')
    #time.sleep(5)


    return data

def ninja_get_wind(lat, lon, from_date='2014-01-01', to_date='2014-12-31', dataset='merra2', capacity=1, height=60, turbine='Vestas+V80+2000', raw='false'):
    url = ninja_build_wind_url(lat, lon, from_date, to_date, dataset, capacity, height, turbine, raw, 'csv')
    return ninja_get_url(url)

def ninja_get_solar(lat, lon, from_date='2014-01-01', to_date='2014-12-31', dataset='merra2', capacity=1, system_loss=0.1, tracking=0, tilt=35, azim=180, raw='false'):
    url = ninja_build_solar_url(lat, lon, from_date, to_date, dataset, capacity, system_loss, tracking, tilt, azim, raw, 'csv')
    return ninja_get_url(url)

def ninja_get_weather(lat, lon, from_date='2014-01-01', to_date='2014-12-31',dataset='merra2',var_t2m='true',var_prectotland='true',var_precsnoland='true',var_snomas='true',var_rhoa='true',var_swgdn='true',var_swtdn='true',var_cldtot='true',raw='false'):
    url = ninja_build_weather_url(lat, lon, from_date, to_date,dataset,var_t2m,var_prectotland,var_precsnoland,var_snomas,var_rhoa,var_swgdn,var_swtdn,var_cldtot, raw, 'csv')
    return ninja_get_url(url)


def ninja_aggregate_urls(urls, names=None):
    n = len(urls)
    if names and len(names) != n:
        raise ValueError("Names should be a list the same length as urls / lat and lon!")

    all_farms = None
    print(urls)
    for i, url in enumerate(urls):
        #print(i)
        print(f"Downloading farm {i+1} of {n}", end='\r')
        this_farm = ninja_get_url(url)
        #print(this_farm)
        header = this_farm.columns.tolist()
        #print(header)


        if all_farms is None:
            all_farms = pd.DataFrame(this_farm.iloc[:, :])

        else:
            all_farms = pd.concat([all_farms, this_farm.iloc[:, 1:]], axis=1)


    #all_farms.insert(0, this_farm.iloc[:, 0].name, this_farm.iloc[:, 0])
    if names:
        #all_farms.columns = [this_farm.columns[0]] + [name for name in names]
        all_farms.columns = [this_farm.columns[0]] + [f"{name}+{j}" for name in names for j in range(len(header))]

    else:
        #all_farms.columns = [this_farm.columns[0]] + [f'output_{i+1}' for i in range(n)]
        all_farms.columns = [this_farm.columns[0]] + [f'output_{i + 1}+{header[j+1]}' for i in range(n) for j in range(len(header)-1)]

    print(f"Downloaded {n} farms...\n")
    return all_farms

def ninja_aggregate_wind(lat, lon, from_date='2014-01-01', to_date='2014-12-31', dataset='merra2', capacity=1, height=60, turbine='Vestas+V80+2000', names=None):
    # Ensure lat and lon are lists
    if isinstance(lat, (float, int)):
        lat = [lat]
    if isinstance(lon, (float, int)):
        lon = [lon]

    if len(lat) != len(lon):
        raise ValueError("Lat and Lon should be lists of the same length!")

    if not isinstance(from_date, str) or not isinstance(to_date, str):
        raise ValueError("From and To dates should be single date strings!")
    #length_parms = [len(dataset), len(capacity), len(height), len(turbine)]
    length_parms = [
        dataset if isinstance(dataset, list) else [dataset],
        capacity if isinstance(capacity, list) else [capacity],
        height if isinstance(height, list) else [height],
        turbine if isinstance(turbine, list) else [turbine]
    ]

    if not all(len(param) == 1 or len(param) == len(lat) for param in length_parms):
        raise ValueError("Farm parameters should either be single values or lists of the same length as lat and lon!")

    # if not all(x in [1, len(lat)] for x in length_parms):
    #     raise ValueError("Farm parameters should either be single values or lists of the same length as lat and lon!")

    urls = [ninja_build_wind_url(lat[i], lon[i], from_date, to_date, dataset, capacity, height, turbine) for i in range(len(lat))]
    return ninja_aggregate_urls(urls, names)

def ninja_aggregate_solar(lat, lon, from_date='2014-01-01', to_date='2014-12-31', dataset='merra2', capacity=1, system_loss=0.1, tracking=0, tilt=35, azim=180, names=None):
    # Ensure lat and lon are lists
    if isinstance(lat, (float, int)):
        lat = [lat]
    if isinstance(lon, (float, int)):
        lon = [lon]

    if len(lat) != len(lon):
        raise ValueError("Lat and Lon should be lists of the same length!")

    if not isinstance(from_date, str) or not isinstance(to_date, str):
        raise ValueError("From and To dates should be single date strings!")
    #length_parms = [len(dataset), len(capacity), len(system_loss), len(tracking), len(tilt), len(azim)]
    length_parms = [
        dataset if isinstance(dataset, list) else [dataset],
        capacity if isinstance(capacity, list) else [capacity],
        system_loss if isinstance(system_loss, list) else [system_loss],
        tracking if isinstance(tracking, list) else [tracking],
        tilt if isinstance(tilt, list) else [tilt],
        azim if isinstance(azim, list) else [azim]
    ]

    if not all(len(param) == 1 or len(param) == len(lat) for param in length_parms):
        raise ValueError("Farm parameters should either be single values or lists of the same length as lat and lon!")

    # if not all(x in [1, len(lat)] for x in length_parms):
    #     raise ValueError("Farm parameters should either be single values or lists of the same length as lat and lon!")

    urls = [ninja_build_solar_url(lat[i], lon[i], from_date, to_date, dataset, capacity, system_loss, tracking, tilt, azim) for i in range(len(lat))]
    return ninja_aggregate_urls(urls, names)


def ninja_aggregate_weather(lat, lon, from_date='2014-01-01', to_date='2014-12-31',dataset='merra2',var_t2m='true',var_prectotland='true',var_precsnoland='true',var_snomas='true',var_rhoa='true',var_swgdn='true',var_swtdn='true',var_cldtot='true',names=None):
    if isinstance(lat, (float, int)):
        lat = [lat]
    if isinstance(lon, (float, int)):
        lon = [lon]
    if len(lat) != len(lon):
        raise ValueError("Lat and Lon should be lists of the same length!")

    if not isinstance(from_date, str) or not isinstance(to_date, str):
        raise ValueError("From and To dates should be single date strings!")
    length_parms = [
        dataset if isinstance(dataset, list) else [dataset],
        var_t2m if isinstance(var_t2m, list) else [var_t2m],
        var_prectotland if isinstance(var_prectotland, list) else [var_prectotland],
        var_precsnoland if isinstance(var_precsnoland, list) else [var_precsnoland],
        var_snomas if isinstance(var_snomas, list) else [var_snomas],
        var_rhoa if isinstance(var_rhoa, list) else [var_rhoa],
        var_swgdn if isinstance(var_swgdn, list) else [var_swgdn],
        var_swtdn if isinstance(var_swtdn, list) else [var_swtdn],
        var_cldtot if isinstance(var_cldtot, list) else [var_cldtot],

    ]

    if not all(len(param) == 1 or len(param) == len(lat) for param in length_parms):
        raise ValueError("Farm parameters should either be single values or lists of the same length as lat and lon!")

    urls = [ninja_build_weather_url(lat[i], lon[i], from_date, to_date, dataset, var_t2m, var_prectotland, var_precsnoland, var_snomas, var_rhoa,var_swgdn,var_swtdn,var_cldtot) for i in range(len(lat))]
    return ninja_aggregate_urls(urls, names)



#
# if __name__ == "__main__":
    # Example usage
    # lat = [-39.23,-40,-48]
    # lon = [149.05,150,162]
    # lat = -39.23
    # lon = 149.05
    #weather_data = ninja_aggregate_weather(lat, lon, from_date='2020-02-02', to_date='2020-02-03')

    #weather_data = ninja_aggregate_weather(lat, lon, from_date='2023-01-01', to_date='2023-12-31')
    #wind_data = ninja_get_wind(lat, lon, from_date='2020-01-01', to_date='2020-01-02')
    #wind_data = ninja_aggregate_wind(lat, lon, from_date='2023-01-01', to_date='2023-12-31')
    #solar_data = ninja_aggregate_solar(lat, lon, from_date='2020-01-01', to_date='2020-12-31')
    # print(weather_data.head())  # Print the first few rows of the data
    #print(weather_data)
    #weather_data.to_csv('rubbish.csv', index=False)
    #print("Done")

    #print(wind_data.head())
    # print(solar_data.head())
    # print(solar_data)


#  https://www.renewables.ninja/api/data/weather?lat=-37.23&lon=143.05&date_from=2020-01-01&date_to=2020-01-31&dataset=merra2&var_t2m=true&var_prectotland=true&var_precsnoland=true&var_snomas=true&var_rhoa=true&var_swgdn=true&var_swtdn=true&var_cldtot=true
# https://www.renewables.ninja/api/data/weather?local_time=true&format=csv&header=true&lat=-8.13824429430089&lon=144.4726562500026&date_from=2023-01-01&date_to=2023-12-31&dataset=merra2&var_t2m=true&var_prectotland=true&var_precsnoland=true&var_snomas=true&var_rhoa=true&var_swgdn=true&var_swtdn=true&var_cldtot=true