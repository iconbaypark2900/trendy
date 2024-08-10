import networkx as nx
import logging

# Setup logging
logging.basicConfig(filename='./logs/query_graph.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def bfs_traversal(graph, start_node):
    try:
        visited = []
        queue = [start_node]
    
        while queue:
            node = queue.pop(0)
            if node not in visited:
                visited.append(node)
                neighbors = list(graph.neighbors(node))
                for neighbor in neighbors:
                    queue.append(neighbor)
        return visited
    except Exception as e:
        logging.error(f"Error during BFS traversal: {e}")
        return []

if __name__ == "__main__":
    G = nx.read_gexf('./data/processed/knowledge_graph.gexf')
    start_node = 'AI'  # Example start node
    visited_nodes = bfs_traversal(G, start_node)
    logging.info(f"Nodes visited during traversal starting from {start_node}: {visited_nodes}")
    print(f"Nodes visited: {visited_nodes}")
