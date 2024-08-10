import os
from scripts.fetch_data import fetch_reddit_data

def test_fetch_reddit_data():
    subreddits = ['technology', 'programming']
    data = fetch_reddit_data(subreddits)
    assert data is not None
    assert not data.empty
