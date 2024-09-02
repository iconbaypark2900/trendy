import os
import pandas as pd
import logging
import time
import spacy
from textblob import TextBlob
from pathlib import Path

# Initialize logging
logging.basicConfig(filename='./logs/data_processing.log', level=logging.INFO)

# Function to get data path
def get_data_path(data_type):
    base_path = Path('./data')
    return base_path / data_type

RAW_DATA_PATH = get_data_path('raw')
PROCESSED_DATA_PATH = get_data_path('processed')

nlp = spacy.load('en_core_web_sm')

def normalize_text(text):
    """
    Normalizes the given text by converting it to lowercase, performing lemmatization,
    and replacing certain terms with their standardized equivalents.
    """
    doc = nlp(text.lower())
    synonyms = {
        'ml': 'machine learning',
        'ai': 'artificial intelligence',
        'cloud': 'cloud computing'
    }
    normalized_text = " ".join([synonyms.get(token.text, token.lemma_) for token in doc])
    return normalized_text

def process_ner(text):
    """
    Performs Named Entity Recognition (NER) on the given text using spaCy.
    """
    doc = nlp(text)
    entities = [ent.text for ent in doc.ents if ent.label_ in ['ORG', 'PRODUCT', 'TECH']]
    return entities

def process_google_trends():
    start_time = time.time()
    try:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith('google_trends')]
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            df.columns = [normalize_text(col) for col in df.columns]
            # Check if the column exists
            if 'some_column' not in df.columns:
                logging.warning("Column 'some_column' is missing in the data.")
                continue

            # Check if the sum of the column is zero
            if df['some_column'].sum() == 0:
                logging.warning("Sum of column 'some_column' is zero, skipping division.")
                continue

            # Perform the division
            df['trend_change'] = df.iloc[:, 1:].pct_change(axis='columns').fillna(0)
            filename = os.path.join(PROCESSED_DATA_PATH, f"processed_{file}")
            df.to_csv(filename, index=False)
            logging.info(f"Processed Google Trends data saved to {filename}")
    except Exception as e:
        logging.error(f"Error processing Google Trends data: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Google Trends processing took {elapsed_time:.2f} seconds.")

def process_reddit():
    start_time = time.time()
    try:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith('reddit')]
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            df['title'] = df['title'].apply(normalize_text)
            df['sentiment'] = df['title'].apply(lambda x: TextBlob(x).sentiment.polarity)
            df['entities'] = df['title'].apply(process_ner)
            filename = os.path.join(PROCESSED_DATA_PATH, f"processed_{file}")
            df.to_csv(filename, index=False)
            logging.info(f"Processed Reddit data saved to {filename}")
    except Exception as e:
        logging.error(f"Error processing Reddit data: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Reddit processing took {elapsed_time:.2f} seconds.")

def process_hacker_news():
    start_time = time.time()
    try:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith('hacker_news')]
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            df['title'] = df['title'].apply(normalize_text)
            df['sentiment'] = df['title'].apply(lambda x: TextBlob(x).sentiment.polarity)
            df['entities'] = df['title'].apply(process_ner)
            filename = os.path.join(PROCESSED_DATA_PATH, f"processed_{file}")
            df.to_csv(filename, index=False)
            logging.info(f"Processed Hacker News data saved to {filename}")
    except Exception as e:
        logging.error(f"Error processing Hacker News data: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Hacker News processing took {elapsed_time:.2f} seconds.")

def process_stack_overflow():
    start_time = time.time()
    try:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith('stackoverflow')]
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            df['title'] = df['title'].apply(normalize_text)
            df['keywords'] = df['title'].apply(lambda x: ','.join(TextBlob(x).noun_phrases))
            df['entities'] = df['title'].apply(process_ner)
            filename = os.path.join(PROCESSED_DATA_PATH, f"processed_{file}")
            df.to_csv(filename, index=False)
            logging.info(f"Processed Stack Overflow data saved to {filename}")
    except Exception as e:
        logging.error(f"Error processing Stack Overflow data: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Stack Overflow processing took {elapsed_time:.2f} seconds.")

def process_dev_to():
    start_time = time.time()
    try:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith('dev_to')]
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            df['title'] = df['title'].apply(normalize_text)
            df['keywords'] = df['title'].apply(lambda x: ','.join(TextBlob(x).noun_phrases))
            df['entities'] = df['title'].apply(process_ner)
            filename = os.path.join(PROCESSED_DATA_PATH, f"processed_{file}")
            df.to_csv(filename, index=False)
            logging.info(f"Processed Dev.to data saved to {filename}")
    except Exception as e:
        logging.error(f"Error processing Dev.to data: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Dev.to processing took {elapsed_time:.2f} seconds.")

def process_product_hunt():
    start_time = time.time()
    try:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.startswith('product_hunt')]
        for file in files:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, file))
            df['description'] = df['description'].apply(normalize_text)
            df['sentiment'] = df['description'].apply(lambda x: TextBlob(x).sentiment.polarity)
            df['entities'] = df['description'].apply(process_ner)
            filename = os.path.join(PROCESSED_DATA_PATH, f"processed_{file}")
            df.to_csv(filename, index=False)
            logging.info(f"Processed Product Hunt data saved to {filename}")
    except Exception as e:
        logging.error(f"Error processing Product Hunt data: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f"Product Hunt processing took {elapsed_time:.2f} seconds.")

def process_all_data():
    process_google_trends()
    process_reddit()
    process_hacker_news()
    process_stack_overflow()
    process_dev_to()
    process_product_hunt()
    logging.info("Data processing complete.")