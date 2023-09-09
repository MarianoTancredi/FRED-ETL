import pandas as pd
import logging
import os
from .config import constants

logging.getLogger().setLevel(logging.INFO)

def write_excel(dataframe):
    ''' Writes the dataframe provided into an xlsx file
        params: dataframe - Pandas Dataframe
    '''
    try:
        root_directory = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(root_directory,'files','processed_data','data.xlsx')
        dataframe.to_excel(path,index=False)
    except Exception as error:
        logging.error(f"Error while creating the excel file: {error}")

def read_local_files():
    ''' In case of error. This functions parses the processed data to create a dataframe to upload
        returns: dictionary with observations
    '''
    import json
    observations = {}

    files_folder = os.path.join(os.path.dirname(__file__), 'files','raw')
    for filename in os.listdir(files_folder):
        with open(os.path.join(files_folder, filename), 'r') as file:
            try:
                name = filename.split('.')[0]
                data = json.load(file)
                observations[name] = data['observations']
            
            except json.JSONDecodeError:
                logging.error(f"Error while reading file: {filename}")
            
            finally:
                file.close()

    return observations

def read_observations(observations_dict, write_to_file = True, use_local_files = False):
    ''' Parses a dictionary with the series names as keys and the observations as values.
        params: observations_dict - Dictionary with observations. In case use_local_files is set to True or dictionary is empty, it will trigger an auxiliar function
                write_to_file - In case it is set to True, it will write the processed files into a xlsx in the processed folder
                use_local_files - In case use_local_files is set to True, it will trigger the function read_local_files which uses the data in the raw files folder
    '''
    data_list = []

    if observations_dict == {} or use_local_files:
        logging.warning("Observations Data is empty. Using local files")
        observations_dict = read_local_files()

    observations_by_date = {}

    for key, observations in observations_dict.items():
        for entry in observations:
            try:
                date = entry['date']
                value = entry['value']
                
                if date not in observations_by_date:
                    observations_by_date[date] = {'Observation Date': date}
                observations_by_date[date][key] = value

            except Exception as error:
                logging.warning(f"Error while parsing observation from {key}: {error}. Skipping it")
                pass
        
    df = pd.DataFrame(observations_by_date.values())

    
    logging.info("Finished creating dataset")
    
    df['Observation Date'] = pd.to_datetime(df['Observation Date'])
        
    #Keep only the observations past the 2000
    df = df.loc[df['Observation Date'] > pd.to_datetime(constants.START_DATE)]
    
    if write_to_file:
        logging.info("Writing dataset to Excel")
        write_excel(df)

    
    return df
    
