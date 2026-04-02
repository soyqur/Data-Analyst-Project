
# Importing Libraries
import os
import pandas as pd
import logging
from sqlalchemy import create_engine
import sys
import time


# Fix import path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from config.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


# Create logs folder
logs_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(logs_dir, exist_ok=True)

# Logging setup
logging.basicConfig(
    filename=os.path.join(logs_dir, "processing_data.log"),
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


logging.info(f"\n{'*'*11}Pipeline Started{'*'*11}")


# DB connection
def get_connection():
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

# Extract

def selecting_raw_europe_sales():
    logging.info("Extracting data from MySQL")

    query = '''
        SELECT 
        s.Region,
        s.Country,
        s.`Item Type`,
        s.`Sales Channel`,
        s.`Order Priority`,
        s.`Order Date`,
        s.Date,
        s.`Order ID`,
        s.`Ship Date`,
        s.`Units Sold`,
        s.`Unit Price`,
        s.`Unit Cost`,
        s.`Total Revenue`,
        s.`Total Cost`,
        s.`Total Profit`   
        FROM raw_europe_sales AS s;
    '''

    df = pd.read_sql(query, get_connection())

    logging.info(f"Data extracted: {df.shape[0]} rows")

    return df

# Transform + Save

def cleaned_europe_sales(df):

    logging.info("Cleaning started")

    # Dates
    date_cols = ['Order Date', 'Date', 'Ship Date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Clean only text columns
    text_cols = ['Region', 'Country', 'Item Type',
                 'Sales Channel', 'Order Priority']
    for col in text_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r'[^a-zA-Z\s]', '', regex=True)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )

    # Drop duplicates
    df = df.drop_duplicates()

    # Ensure output folder exists
    output_dir = os.path.join(BASE_DIR, "data", "processed")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, "cleaned_europe_sales.csv")

    df.to_csv(file_path, index=False)

    logging.info(f"Saved cleaned file: {file_path}")

    return df

# Main pipeline


def main():

    start = time.time()

    df = selecting_raw_europe_sales()      # Extract once
    cleaned_df = cleaned_europe_sales(df)  # Pass df

    end = time.time()

    print(f"⏱ Execution time: {end - start:.2f} seconds")
    print(f"✅ Rows processed: {cleaned_df.shape[0]}")

    logging.info(f"\n{'*'*11}Pipeline Ended{'*'*11}")


if __name__ == "__main__":
    main()
