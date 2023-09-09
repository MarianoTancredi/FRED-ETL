import pandas as pd
import pypyodbc as obdc
from sqlalchemy import create_engine
from config import constants
import logging
import os

logging.getLogger().setLevel(logging.INFO)
logging.getLogger('sqlalchemy.engine')
#In case of wanting to show the sql output in the stack, uncomment the next line
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def get_lastest_observation(conn):
    ''' Rretrieves the lastest observation date so that we only insert observations after that date. The query returns a tuple with the value
        params: conn - database connect object
        returns: a date string
    '''
    sql_query = f"SELECT MAX({constants.DATE_COLUMN}) FROM {constants.TABLE_NAME}"
    cursor = conn.cursor()
    cursor.execute(sql_query)
    date_tuple = cursor.fetchone()

    return date_tuple[0]

def read_local_files():
    ''' In case of error. This functions parses the processed data to create a dataframe to upload
        returns: pandas dataframe
    '''
    files_folder = os.path.join(os.path.dirname(__file__), 'files','processed_data')
    excel_file_path = os.path.join(files_folder, 'data.xlsx')
    excel_data = pd.read_excel(excel_file_path)
    dataframe = pd.DataFrame(excel_data)

    return dataframe


def upload_to_db(dataframe,insert_only_new = False, use_local_files = False):
    ''' Uploads dataframe to database. In case of too many observations being uploaded, it can be set to only upload newest observations
        params: Dataframe - Pandas Dataframe with Observations
                insert_only_new - In case it is set to True, it will only slice the dataframe and upload only latest observations. This is useful to avoid having large unnecessary queries
    '''
    
    if use_local_files or not dataframe:
        dataframe = read_local_files()


    connection_string = f"""
        DRIVER={{{constants.DRIVER_NAME}}};
        SERVER={constants.SERVER_NAME};
        DATABASE={constants.DATABASE_NAME};
        Trust_Connection=yes
    """
    logging.info("Connecting to database")
    conn = obdc.connect(connection_string)
    
    if insert_only_new:
        logging.info("Obtaining latest date from database")
        latest_date = get_lastest_observation(conn)

        if latest_date:
            dataframe = dataframe.loc[dataframe['Observation Date'] > pd.to_datetime(latest_date)]

    
    engine = create_engine(constants.SQL_ENGINE)

    logging.info("Uploading Dataframe to database")
    dataframe.to_sql(constants.TABLE_NAME, engine, if_exists='append', index=False)
    logging.info("Uploading finished successfully")


