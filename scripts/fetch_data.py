import os
import pandas as pd
import praw
import logging
import time
from datetime import datetime
from pytrends.request import TrendReq
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(filename='./logs/data_collection.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RAW_DATA_PATH = './data/raw/'

def fetch_google_trends_data(keywords):
    try:
        pytrends = TrendReq(hl='en-US', tz=360)
        pytrends.build_payload(keywords, cat=0, timeframe='now 7-d', geo='', gprop='')
        time.sleep(10)  # Introduce a delay to avoid rate limiting
        trends_data = pytrends.interest_over_time()

        if not trends_data.empty:
            filename = f"{RAW_DATA_PATH}google_trends_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            trends_data.to_csv(filename)
            logging.info(f"Google Trends data saved to {filename}")
        return trends_data
    except Exception as e:
        logging.error(f"Error fetching Google Trends data: {e}")
        return None

def fetch_reddit_data(subreddits):
    try:
        reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        all_data = []
        for subreddit in subreddits:
            for submission in reddit.subreddit(subreddit).top(time_filter='week', limit=50):
                all_data.append([submission.title, submission.score, submission.subreddit, submission.url])
        
        all_data.sort(key=lambda x: x[1], reverse=True)  # Sort by score

        df = pd.DataFrame(all_data, columns=['title', 'score', 'subreddit', 'url'])
        filename = f"{RAW_DATA_PATH}reddit_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Reddit data saved to {filename}")
        return df
    except Exception as e:
        logging.error(f"Error fetching Reddit data: {e}")
        return None

if __name__ == "__main__":
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    # Fetch Google Trends data
    keywords = ['AWS', 'Azure', 'Google Cloud Platform']
    fetch_google_trends_data(keywords)
    
    # Fetch Reddit data
    subreddits = ['technology', 'programming', 'machinelearning']
    fetch_reddit_data(subreddits)
