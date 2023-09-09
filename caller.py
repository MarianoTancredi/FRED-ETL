from util.downloader import download_process
from util.parser import read_observations
from util.sql_loader import upload_to_db
import logging

logging.getLogger().setLevel(logging.INFO)

def call_downloader():
    '''In case of need only raw files. The downloader can be trigged alone'''
    download_process(write_to_file = True)

def call_parser():
    ''' In case of error during workflow or need of processed data, the parser can be triggered alone
        It uses the raw files from the files folder
    '''
    read_observations({},write_to_file=True,use_local_files = True)

def call_loader():
    ''' In case of error during workflow or need of uploading data, the loader can be triggered alone
        It uses the processed files from the files folder
    '''
    upload_to_db(insert_only_new = True,use_local_files = True)

def call_workflow(save_downloader_files,save_processed_files,upload_data,insert_only_new_data = True):
    ''' Main Funcion. Invokes all process functions and works with data in memory.

    '''

    logging.info("Starting Downloading Process")
    try:
        observations = download_process(save_downloader_files)
    except Exception as error:
        logging.error(f"Error during download process: {error}")

    logging.info("Starting Parsing Process")
    try:
        data = read_observations(observations,save_processed_files)
    except Exception as error:
        logging.error(f"Error during parsing process: {error}")
    
    if upload_data:
        logging.info("Starting Parsing Process")
        try:
            upload_to_db(data,insert_only_new_data)
        except Exception as error:
            logging.error(f"Error during parsing process: {error}")


call_workflow(True,True,False)