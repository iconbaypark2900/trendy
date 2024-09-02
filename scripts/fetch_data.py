import os
import requests
import pandas as pd
import praw
import logging
import time
from datetime import datetime
from pytrends.request import TrendReq
from pathlib import Path
from dotenv import load_dotenv
import re

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(filename='./logs/data_collection.log', level=logging.INFO)

# Function to get data path
def get_data_path(data_type):
    base_path = Path('./data')
    return base_path / data_type

RAW_DATA_PATH = get_data_path('raw')

def log_request_response(response):
    logging.info(f"Request URL: {response.request.url}")
    logging.info(f"Request Headers: {response.request.headers}")
    logging.info(f"Request Body: {response.request.body}")
    logging.info(f"Response Status Code: {response.status_code}")
    logging.info(f"Response Headers: {response.headers}")
    logging.info(f"Response Body: {response.text}")

def fetch_google_trends_data(keywords):
    try:
        pytrends = TrendReq(hl='en-US', tz=360)
        pytrends.build_payload(keywords, cat=0, timeframe='now 7-d', geo='', gprop='')
        time.sleep(10)  # Introduce a delay to avoid rate limiting
        trends_data = pytrends.interest_over_time()

        if not trends_data.empty:
            filename = f"{RAW_DATA_PATH}/google_trends_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            trends_data.to_csv(filename)
            logging.info(f"Google Trends data saved to {filename}")
        else:
            logging.warning("Google Trends data is empty.")
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
        filename = f"{RAW_DATA_PATH}/reddit_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Reddit data saved to {filename}")
        return df
    except Exception as e:
        logging.error(f"Error fetching Reddit data: {e}")
        return None

def fetch_hacker_news_data():
    try:
        hn_data = requests.get('https://hacker-news.firebaseio.com/v0/topstories.json').json()[:50]
        articles = []
        for item in hn_data:
            story = requests.get(f'https://hacker-news.firebaseio.com/v0/item/{item}.json').json()
            articles.append({
                'title': story['title'],
                'score': story['score'],
                'url': story.get('url', ''),
                'time': datetime.fromtimestamp(story['time'])
            })
        df = pd.DataFrame(articles)
        filename = f"{RAW_DATA_PATH}/hacker_news_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Hacker News data saved to {filename}")
        return df
    except Exception as e:
        logging.error(f"Error fetching Hacker News data: {e}")
        return None

def fetch_stack_overflow_data(tags):
    stackoverflow_data = []
    try:
        for tag in tags:
            response = requests.get(f'https://api.stackexchange.com/2.2/questions?order=desc&sort=activity&tagged={tag}&site=stackoverflow')
            for item in response.json().get('items', []):
                stackoverflow_data.append({
                    'title': item['title'],
                    'score': item['score'],
                    'tags': ','.join(item['tags']),
                    'is_answered': item['is_answered'],
                    'view_count': item['view_count'],
                    'link': item['link']
                })
        df = pd.DataFrame(stackoverflow_data)
        filename = f"{RAW_DATA_PATH}/stackoverflow_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Stack Overflow data saved to {filename}")
        return df
    except Exception as e:
        logging.error(f"Error fetching Stack Overflow data: {e}")
        return None

def fetch_dev_to_data():
    devto_data = []
    try:
        tags = ['react', 'vue', 'node', 'django', 'flask']
        for tag in tags:
            response = requests.get(f'https://dev.to/api/articles?tag={tag}')
            for article in response.json():
                devto_data.append({
                    'title': article['title'],
                    'published_at': article['published_at'],
                    'tag_list': ','.join(article['tag_list']),
                    'positive_reactions_count': article['positive_reactions_count'],
                    'url': article['url']
                })
        df = pd.DataFrame(devto_data)
        filename = f"{RAW_DATA_PATH}/dev_to_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Dev.to data saved to {filename}")
        return df
    except Exception as e:
        logging.error(f"Error fetching Dev.to data: {e}")
        return None

def fetch_product_hunt_data():
    try:
        product_hunt_api_key = os.getenv('PRODUCT_HUNT_API_KEY')
        headers = {
            'Authorization': f'Bearer {product_hunt_api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        response = requests.get('https://api.producthunt.com/v2/api/graphql', headers=headers)
        products = response.json().get('data', {}).get('products', [])
        product_data = []
        for product in products:
            product_data.append({
                'name': product['name'],
                'description': product['tagline'],
                'votes': product['votesCount'],
                'url': product['website'],
                'created_at': product['createdAt']
            })
        df = pd.DataFrame(product_data)
        filename = f"{RAW_DATA_PATH}/product_hunt_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Product Hunt data saved to {filename}")
        return df
    except Exception as e:
        logging.error(f"Error fetching Product Hunt data: {e}")
        return None

def collect_all_data(expanded_categories):
    logging.info("Starting data collection...")
    fetch_google_trends_data(expanded_categories['Trend Analysis (Google Trends)'])
    fetch_reddit_data(expanded_categories['Community Insights and Sentiment (Reddit)'])
    fetch_hacker_news_data()
    fetch_stack_overflow_data(expanded_categories['Technical Support and Skill Development (Stack Overflow)'])
    fetch_dev_to_data()
    fetch_product_hunt_data()
    logging.info("Data collection complete.")

# Example usage
if __name__ == "__main__":
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    expanded_categories = {
        "Trend Analysis (Google Trends)": [
            'AWS', 'Azure', 'Google Cloud Platform', 'IBM Cloud', 'Alibaba Cloud', # and others...
        ],
        "Community Insights and Sentiment (Reddit)": [
            'r/technology', 'r/programming', 'r/webdev', 'r/machinelearning', 'r/datascience', # and others...
        ],
        "Tech News and Innovations (Hacker News)": [
            'Go', 'Blockchain', 'AI', 'Cloud Computing', 'React', 'Flutter', # and others...
        ],
        "Technical Support and Skill Development (Stack Overflow)": [
            'Python', 'JavaScript', 'Java', 'SQL', 'C#', # and others...
        ],
        "Developer Opinions and Tutorials (Dev.to)": [
            'React', 'Vue.js', 'Node.js', 'Django', 'Flask', # and others...
        ],
        "Product Hunt": [
            'Tech', 'Startups', 'Apps', 'Gadgets', 'Software'  # Example categories for Product Hunt
        ]
    }

    collect_all_data(expanded_categories)
