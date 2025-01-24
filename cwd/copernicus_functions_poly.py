from collections import namedtuple
from datetime import datetime
import os
import pandas as pd
from shapely.geometry import Point


def __readIndexFileFromCWD(filepath, polygon):
    """ Load as pandas dataframe the file in the provided path """
    filename = os.path.basename(filepath)
    print('...Loading info from: '+filename)
    if polygon != None:
        raw_index_info =[]
        chunks = pd.read_csv(filepath, skiprows=5,chunksize=1000)
        for chunk in chunks:
            chunk['poligonOverlap'] = chunk.apply(poligonOverlap, polygon=polygon, axis=1)
            raw_index_info.append(chunk[chunk['poligonOverlap'] == True])
        return pd.concat(raw_index_info)
    else:
        result = pd.read_csv(filepath, skiprows=5)
        try:
            result = result.rename(columns={"provider_edmo_code": "institution_edmo_code"})
        except Exception as e:
            pass
        return result


def poligonOverlap(row, polygon):
    """ Checks if a file contains data in the specified area (polygon) """
    result = False
    try:
        geospatial_lat_min = float(row['geospatial_lat_min'])
        geospatial_lon_min = float(row['geospatial_lon_min'])
        targeted_point = Point(geospatial_lat_min,geospatial_lon_min)
        if targeted_point.within(polygon):
            result = True
    except Exception as e:
        pass
    return result


def rangeBounds(range: str):
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    start = datetime.strptime(range.split('/')[0], date_format)
    end = datetime.strptime(range.split('/')[1], date_format)
    return (start, end)


def timeOverlap(row, start, end):
    """ Checks if a file contains data in the specified time range (targeted_range) """
    result = False
    try:
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        time_start = datetime.strptime(row['time_coverage_start'],date_format)
        time_end = datetime.strptime(row['time_coverage_end'],date_format)
        Range = namedtuple('Range', ['start', 'end'])
        r1 = Range(start=start, end=end)
        r2 = Range(start=time_start, end=time_end)
        latest_start = max(r1.start, r2.start)
        earliest_end = min(r1.end, r2.end)
        delta = (earliest_end - latest_start).days + 1
        overlap = max(0, delta)
        if overlap != 0:
            result = True
    except Exception as e:
        print('ERROR timeOverlap')
        pass
    return result


def getIndexFilesInfo(dataset, local_dir, polygon):
    # Load and merge in a single entity all the information contained on each file descriptor of a given dataset
    # 1) Loading the index platform info as dataframe
    filepath = os.path.join(local_dir, dataset['index_platform'])
    indexPlatform = __readIndexFileFromCWD(filepath, None)
    indexPlatform.rename(columns={indexPlatform.columns[0]: "platform_code" }, inplace = True)
    indexPlatform = indexPlatform.drop_duplicates(subset='platform_code', keep="first")
    # 2) Loading the index files info as dataframes
    netcdf_collections = []
    for filename in dataset['index_files']:
        filepath = os.path.join(local_dir,filename)
        indexFile = __readIndexFileFromCWD(filepath, polygon)
        netcdf_collections.append(indexFile)
    netcdf_collections = pd.concat(netcdf_collections)
    # 3) creating new columns: derived info
    netcdf_collections['netcdf'] = netcdf_collections['file_name'].str.split('/').str[-1]
    netcdf_collections['file_type'] = netcdf_collections['netcdf'].str.split('.').str[0].str.split('_').str[1]
    netcdf_collections['data_type'] = netcdf_collections['netcdf'].str.split('.').str[0].str.split('_').str[2]
    netcdf_collections['platform_code'] = netcdf_collections['netcdf'].str.split('.').str[0].str.split('_').str[3]
    # 4) Merging the information of all files
    headers = ['platform_code','wmo_platform_code', 'institution_edmo_code', 'parameters', 'last_latitude_observation', 'last_longitude_observation','last_date_observation']
    result = pd.merge(netcdf_collections,indexPlatform[headers],on='platform_code')
    return result
