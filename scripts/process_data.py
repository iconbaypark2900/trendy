import os
import pandas as pd
import logging

RAW_DATA_PATH = './data/raw/'
PROCESSED_DATA_PATH = './data/processed/'

# Setup logging
logging.basicConfig(filename='./logs/data_collection.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_google_trends_data():
    try:
        files = [file for file in os.listdir(RAW_DATA_PATH) if 'google_trends' in file]
        if not files:
            raise ValueError("No Google Trends data files found to process.")

        dataframes = []
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file), index_col=0)
            if not df.empty:
                dataframes.append(df)

        if not dataframes:
            raise ValueError("No valid Google Trends data found.")

        combined_df = pd.concat(dataframes, axis=0)
        processed_filename = f"{PROCESSED_DATA_PATH}processed_google_trends.csv"
        combined_df.to_csv(processed_filename)
        logging.info(f"Processed Google Trends data saved to {processed_filename}")
        return combined_df
    except Exception as e:
        logging.error(f"Error processing Google Trends data: {e}")
        return None

def process_reddit_data():
    try:
        files = [file for file in os.listdir(RAW_DATA_PATH) if 'reddit' in file]
        dataframes = []
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            if not df.empty:
                dataframes.append(df)
        
        if not dataframes:
            raise ValueError("No valid Reddit data found.")

        combined_df = pd.concat(dataframes, axis=0)
        processed_filename = f"{PROCESSED_DATA_PATH}processed_reddit.csv"
        combined_df.to_csv(processed_filename)
        logging.info(f"Processed Reddit data saved to {processed_filename}")
        return combined_df
    except Exception as e:
        logging.error(f"Error processing Reddit data: {e}")
        return None

if __name__ == "__main__":
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    process_google_trends_data()
    process_reddit_data()
