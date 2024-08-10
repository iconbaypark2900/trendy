import networkx as nx
import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(filename='./logs/knowledge_graph.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROCESSED_DATA_PATH = './data/processed/'

def build_knowledge_graph():
    try:
        G = nx.Graph()

        google_trends_path = f"{PROCESSED_DATA_PATH}processed_google_trends.csv"
        if not os.path.exists(google_trends_path):
            raise FileNotFoundError(f"Processed Google Trends file not found: {google_trends_path}")

        google_trends_df = pd.read_csv(google_trends_path, index_col=0)
        reddit_df = pd.read_csv(f"{PROCESSED_DATA_PATH}processed_reddit.csv")

        for keyword in google_trends_df.columns[:-1]:  # Exclude 'isPartial' column
            G.add_node(keyword)
            for other_keyword in google_trends_df.columns[:-1]:
                if keyword != other_keyword:
                    G.add_edge(keyword, other_keyword, relation='co-trend')

        for _, row in reddit_df.iterrows():
            G.add_node(row['title'])
            G.add_edge(row['subreddit'], row['title'], relation='discussed_in')

        nx.write_gexf(G, f"{PROCESSED_DATA_PATH}knowledge_graph.gexf")
        logging.info("Knowledge graph saved as knowledge_graph.gexf")
        return G
    except Exception as e:
        logging.error(f"Error building knowledge graph: {e}")
        return None

if __name__ == "__main__":
    build_knowledge_graph()
