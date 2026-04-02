import os
import pandas as pd
import logging
from sqlalchemy import create_engine
import sys
import time
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


print(sys.path)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(f"filepath:{BASE_DIR}")

# Log folder Setup
os.makedirs(os.path.join(BASE_DIR, 'logs'), exist_ok=True)

# Making config folder
# os.makedirs(os.path.join(BASE_DIR, 'config'))

# Logging setup
logging.basicConfig(
    filename=os.path.join(BASE_DIR, "logs", "ingestion.log"),
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode='a'
)

logging.info("Log File Created")


# Database Connection
def get_connection():

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    return engine



# Function

# This function sends Pandas data frame to MySQL table
def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)



def load_raw_data():

    # Create database connection
    engine = get_connection()

    # Raw data folder path
    data_path = os.path.join(BASE_DIR, "data", "raw")

    for file in os.listdir(data_path):
        try:

            start = time.time()

            df = pd.read_csv(os.path.join(data_path, file))

            # Send dataframe to MySQL
            ingest_db(df, file[:-4], engine)

            end = time.time()

            total_time = (end - start) / 60

            logging.info(f"{file} loaded successfully in {total_time:.2f} minutes")

        except Exception as e:

            logging.error(f"Error loading {file}: {e}")


if __name__ == "__main__":
    load_raw_data()
