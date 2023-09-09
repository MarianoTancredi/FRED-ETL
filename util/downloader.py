import json
import requests
import pandas as pd
import os
from .config import constants
import logging

logging.getLogger().setLevel(logging.INFO)

def read_mapping_file():
    ''' Parses the series mapping file into a dictionary
        returns: Dictionary
    '''
    
    config_folder = os.path.join(os.path.dirname(__file__), 'config')
    excel_file_path = os.path.join(config_folder, 'mapping.xlsx')
    excel_data = pd.read_excel(excel_file_path)
    data = pd.DataFrame(excel_data,columns=['SeriesId','SeriesName','Type','Frequency'])
    
    series_id = []

    for index, row in data.iterrows():
        series_id.append({
            'SeriesId': row['SeriesId'],
            'SeriesName': data.get(row['SeriesName'], row['SeriesName']),
            'Type': constants.UNIT_TRANSLATIONS.get(row['Type'], row['Type']),
            'Frequency': constants.FREQUENCY_TRANSLATIONS.get(row['Frequency'], row['Frequency'])
        })
    
    return series_id

def write_json(series_name,json_content):
    ''' Writes JSON files in the raw folder
        params: series_name - Complete name of the Series/Title for the JSON file
                json_content - Dict or JSON content to write
    '''
    try:
        logging.info(f"Writing {series_name}")
        root_directory = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(root_directory,"files","raw",series_name+".json")
        jsonString = json.dumps(json_content)
        jsonFile = open(path, "w")
        jsonFile.write(jsonString)
        jsonFile.close()
        logging.info(f"Finished writing of {series_name} JSON")
    except Exception as error:
        logging.error(f"Error while writing {series_name} file: {error}")

def download_series(series, write_to_file = True):
    ''' Download series observations. It replaces the parameters in the URL from the series dictionary
        params: series - Dictionary with series information (It must contain: SeriesId,Frequency,Type and SeriesName)
    '''
    series_observations = {}
    for serie in series:
        url = constants.URL_TEMPLATE.format(SeriesId=serie['SeriesId'], Frequency=serie['Frequency'], Type=serie['Type'], API_KEY=constants.API_KEY)
        series_name = serie["SeriesName"]
        logging.info(f"Downloading {series_name}")
        response = requests.get(url)
        
        if response.status_code == 200:
            series_observations[series_name] = (response.json())['observations']
            if write_to_file:
                write_json(series_name,response.json())
        else:
            logging.error(f"Skipping file: {series_name} - Responses Status: {response.status_code}")
            continue
    
    return series_observations




def download_process(write_to_file):
    series = read_mapping_file()
    return download_series(series,write_to_file)
