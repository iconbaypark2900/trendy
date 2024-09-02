import os
import logging
import networkx as nx
from dotenv import load_dotenv
from scripts.fetch_data import fetch_google_trends_data, fetch_reddit_data, fetch_hacker_news_data
from scripts.process_data import process_google_trends, process_reddit, process_hacker_news
from scripts.build_graph import build_knowledge_graph as build_kg, save_graph

# Load environment variables from .env file
load_dotenv()

# Setup logging to ensure all logs go to data_collection.log
logging.basicConfig(filename='./logs/data_collection.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Data Integrity Check
def check_data_integrity():
    required_files = ['google_trends', 'reddit', 'hacker_news']
    missing_files = [f for f in required_files if not any(f in filename for filename in os.listdir('./data/raw/'))]

    if missing_files:
        logging.error(f"Missing required data files: {', '.join(missing_files)}")
        return False
    return True

# Step 1: Fetch Data
def fetch_data():
    try:
        keywords = ['AWS', 'Azure', 'Google Cloud Platform']
        fetch_google_trends_data(keywords)
        logging.info("Google Trends data fetched successfully.")

        subreddits = ['technology', 'programming', 'machinelearning']
        fetch_reddit_data(subreddits)

        fetch_hacker_news_data()
        logging.info("Data fetched successfully.")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise

# Step 2: Process Data
def process_data():
    try:
        process_google_trends()
        process_reddit()
        process_hacker_news()
        logging.info("Data processed successfully.")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

# Step 3: Build Knowledge Graph
def build_and_save_knowledge_graph():
    try:
        graph = nx.Graph()
        build_kg(graph)
        save_graph(graph)
        logging.info("Knowledge graph built and saved successfully.")
    except Exception as e:
        logging.error(f"Error building knowledge graph: {e}")
        raise

# Run Pipeline
def run_pipeline():
    logging.info("Pipeline execution started.")
    ensure_directories()
    fetch_data()
    if check_data_integrity():
        process_data()
        build_and_save_knowledge_graph()
        logging.info("Pipeline executed successfully.")
    else:
        logging.error("Pipeline execution halted due to data integrity issues.")

# Example usage
if __name__ == "__main__":
    run_pipeline()