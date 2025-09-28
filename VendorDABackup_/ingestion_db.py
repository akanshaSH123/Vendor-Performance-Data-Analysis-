import pandas as pd
import os 
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename ="logs/ingestion_db.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode ="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, connection):
    '''This function will drop the table if it exists and then ingest the dataframe into database'''
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.commit()
        logging.info(f"Table '{table_name}' dropped successfully (if it existed).")
    except Exception as e:
        logging.error(f"Error dropping table {table_name}: {e}")
        raise

    df.to_sql(table_name, con=connection, if_exists='replace', index=False)
    logging.info(f"Table '{table_name}' ingested successfully.")

def load_raw_data():
    '''This function will load the CSV's as dataframeand ingest into db'''
    start = time.time()
    for file in os.listdir('Data') :
        if '.csv' in file:
            df = pd.read_csv('Data/' +file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time =(end - start)/60
    logging.info('Ingestion Complete')
    logging.info(f'\nTotal Time taken: {total_time} minutes')

if __name__ == '__main__' :
    load_raw_data()