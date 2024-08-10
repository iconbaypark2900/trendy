import os
import logging
from dotenv import load_dotenv
from scripts.fetch_data import fetch_google_trends_data, fetch_reddit_data
from scripts.process_data import process_google_trends_data, process_reddit_data
from scripts.build_graph import build_knowledge_graph

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(filename='./logs/pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure necessary directories exist
def ensure_directories():
    try:
        os.makedirs('./data/raw/', exist_ok=True)
        os.makedirs('./data/processed/', exist_ok=True)
        os.makedirs('./logs/', exist_ok=True)
        logging.info("Required directories ensured.")
    except Exception as e:
        logging.error(f"Error ensuring directories: {e}")
        raise

# Step 1: Fetch Data
def fetch_data():
    try:
        keywords = ['AWS', 'Azure', 'Google Cloud Platform']
        fetch_google_trends_data(keywords)
        logging.info("Google Trends data fetched successfully.")

        subreddits = ['technology', 'programming', 'machinelearning']
        fetch_reddit_data(subreddits)
        logging.info("Reddit data fetched successfully.")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise

# Step 2: Process Data
def process_data():
    try:
        process_google_trends_data()
        logging.info("Google Trends data processed successfully.")

        process_reddit_data()
        logging.info("Reddit data processed successfully.")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

# Step 3: Build Knowledge Graph
def build_graph():
    try:
        build_knowledge_graph()
        logging.info("Knowledge graph built successfully.")
    except Exception as e:
        logging.error(f"Error building knowledge graph: {e}")
        raise

if __name__ == "__main__":
    try:
        logging.info("Pipeline execution started.")
        
        ensure_directories()
        fetch_data()
        process_data()
        build_graph()
        
        logging.info("Pipeline execution completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        print(f"Pipeline execution failed: {e}")
