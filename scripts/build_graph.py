import os
import networkx as nx
import pandas as pd
import logging
from pathlib import Path

# Initialize logging
logging.basicConfig(filename='./logs/build_graph.log', level=logging.INFO)

# Function to get data path
def get_data_path(data_type):
    base_path = Path('./data')
    return base_path / data_type

PROCESSED_DATA_PATH = get_data_path('processed')

def add_nodes_and_edges(graph, data, category):
    """
    Add nodes and edges to the graph based on the collected data.
    """
    if data is not None:
        for index, row in data.iterrows():
            if 'title' in row:
                title = row['title']
                graph.add_node(title, category=category)
                if 'keywords' in row:
                    keywords = row['keywords'].split(',')
                    for keyword in keywords:
                        graph.add_node(keyword, category='Keyword')
                        graph.add_edge(title, keyword, relation='Contains')
                if 'entities' in row:
                    entities = row['entities']
                    for entity in entities:
                        graph.add_node(entity, category='Entity')
                        graph.add_edge(title, entity, relation='Mentions')
            if 'subreddit' in row:
                subreddit = row['subreddit']
                graph.add_node(subreddit, category='Subreddit')
                graph.add_edge(subreddit, title, relation='Discusses')

def build_knowledge_graph(graph):
    sources = ['google_trends', 'reddit', 'hacker_news', 'stackoverflow', 'dev_to', 'product_hunt']
    for source in sources:
        files = [f for f in os.listdir(PROCESSED_DATA_PATH) if f.startswith(f'processed_{source}')]
        for file in files:
            logging.info(f"Processing file: {file}")
            df = pd.read_csv(os.path.join(PROCESSED_DATA_PATH, file))
            add_nodes_and_edges(graph, df, source)
    logging.info(f"Graph built with {len(graph.nodes())} nodes and {len(graph.edges())} edges.")

def save_graph(graph, path='./data/processed/knowledge_graph.gexf'):
    nx.write_gexf(graph, path)
    logging.info(f"Knowledge graph saved to {path}.")

# Example usage
if __name__ == "__main__":
    graph = nx.Graph()
    build_knowledge_graph(graph)
    save_graph(graph)