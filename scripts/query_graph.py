import networkx as nx
import logging
from pathlib import Path

# Initialize logging
logging.basicConfig(filename='./logs/query_graph.log', level=logging.INFO)

# Function to get data path
def get_data_path(data_type):
    base_path = Path('./data')
    return base_path / data_type

class KnowledgeGraphQuery:
    def __init__(self, graph_path: str):
        self.graph = self._load_graph(graph_path)

    def _load_graph(self, graph_path: str) -> nx.Graph:
        try:
            graph = nx.read_gexf(graph_path)
            logging.info(f"Graph loaded successfully with {len(graph.nodes())} nodes and {len(graph.edges())} edges.")
            return graph
        except Exception as e:
            logging.error(f"Failed to load graph: {e}")
            raise

    def bfs_traversal(self, start_node):
        try:
            visited = []
            queue = [start_node]

            while queue:
                node = queue.pop(0)
                if node not in visited:
                    visited.append(node)
                    neighbors = list(self.graph.neighbors(node))
                    queue.extend(neighbors)

            logging.info(f"BFS traversal from node {start_node} visited {len(visited)} nodes.")
            return visited
        except Exception as e:
            logging.error(f"Error during BFS traversal: {e}")
            return []

    def centrality_query(self):
        """
        Finds the most central nodes in the graph based on degree centrality.
        """
        try:
            centrality = nx.degree_centrality(self.graph)
            sorted_nodes = sorted(centrality.items(), key=lambda x: x[1], reverse=True)
            logging.info("Centrality query completed.")
            return sorted_nodes[:10]  # Top 10 central nodes
        except Exception as e:
            logging.error(f"Error during centrality query: {e}")
            return []

    def shortest_path_query(self, node_a, node_b):
        """
        Finds the shortest path between two nodes in the graph.
        """
        try:
            path = nx.shortest_path(self.graph, source=node_a, target=node_b)
            logging.info(f"Shortest path between {node_a} and {node_b} is {path}")
            return path
        except Exception as e:
            logging.error(f"Error finding shortest path: {e}")
            return []

# Example usage
if __name__ == "__main__":
    query = KnowledgeGraphQuery('./data/processed/knowledge_graph.gexf')
    top_nodes = query.centrality_query()
    print(f"Top central nodes: {top_nodes}")
