
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine, chunk_size=1000):
    '''this function will ingest the dataframe into database table'''
    try:
        df.to_sql(
            table_name,
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=chunk_size,
            method='multi'  # optional: speeds up insert for supported DBs
        )
        print(f"✅ Data ingested into {table_name} table.")
    except Exception as e:
        print(f"❌ Failed to ingest {table_name}: {e}")

def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv(os.path.join('data', file))
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file.split('.')[0], engine)
        end = time.time()
        total_time = (end - start)/60
        logging.info('--------------Ingestion Complete---------------')
        logging.info(f'Total Time Taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()