import networkx as nx
import plotly.graph_objs as go
from networkx.algorithms.community import greedy_modularity_communities
import logging
from pathlib import Path

# Initialize logging
logging.basicConfig(filename='./logs/visualize_graph.log', level=logging.INFO)

# Function to get data path
def get_data_path(data_type):
    base_path = Path('./data')
    return base_path / data_type

class AdvancedKnowledgeGraphVisualizer:
    def __init__(self, graph_path: str):
        self.graph = self._load_graph(graph_path)
        self.pos = self._compute_layout()
        self.communities = self._detect_communities()

    def _load_graph(self, graph_path: str) -> nx.Graph:
        try:
            graph = nx.read_gexf(graph_path)
            logging.info(f"Graph loaded successfully with {len(graph.nodes())} nodes and {len(graph.edges())} edges.")
            return graph
        except Exception as e:
            logging.error(f"Failed to load graph: {e}")
            raise

    def _compute_layout(self) -> dict:
        try:
            layout = nx.spring_layout(self.graph, k=0.5, iterations=100)
            logging.info("Graph layout computed successfully.")
            return layout
        except Exception as e:
            logging.error(f"Failed to compute layout: {e}")
            raise

    def _detect_communities(self):
        communities = list(greedy_modularity_communities(self.graph))
        logging.info(f"Detected {len(communities)} communities.")
        return communities

    def _create_edge_trace(self) -> go.Scatter:
        edge_trace = go.Scatter(
            x=[], y=[],
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines')

        for edge in self.graph.edges():
            x0, y0 = self.pos[edge[0]]
            x1, y1 = self.pos[edge[1]]
            edge_trace['x'] += tuple([x0, x1, None])
            edge_trace['y'] += tuple([y0, y1, None])

        logging.info("Edge trace created.")
        return edge_trace

    def _create_node_trace(self) -> go.Scatter:
        node_trace = go.Scatter(
            x=[], y=[],
            text=[], mode='markers', hoverinfo='text',
            marker=dict(
                showscale=True,
                colorscale='Viridis',
                size=[],
                color=[],
                colorbar=dict(
                    thickness=15,
                    title='Category',
                    xanchor='left',
                    titleside='right'
                ),
                line_width=2))

        for node in self.graph.nodes():
            x, y = self.pos[node]
            node_trace['x'] += tuple([x])
            node_trace['y'] += tuple([y])
            node_trace['text'] += tuple([f"{node} (Category: {self.graph.nodes[node]['category']})"])
            node_trace['marker']['color'] += tuple([hash(self.graph.nodes[node]['category']) % 100])
            node_trace['marker']['size'] += tuple([5 + 3 * self.graph.degree[node]])

        logging.info("Node trace created.")
        return node_trace

    def visualize(self, title: str = "Knowledge Graph Visualization"):
        edge_trace = self._create_edge_trace()
        node_trace = self._create_node_trace()

        fig = go.Figure(data=[edge_trace, node_trace],
                        layout=go.Layout(
                            title=f"<br>{title}",
                            titlefont_size=16,
                            showlegend=False,
                            hovermode='closest',
                            margin=dict(b=0, l=0, r=0, t=40),
                            xaxis=dict(showgrid=False, zeroline=False),
                            yaxis=dict(showgrid=False, zeroline=False),
                            plot_bgcolor='black'
                        ))

        logging.info("Displaying graph visualization.")
        fig.show()

# Example usage
if __name__ == "__main__":
    visualizer = AdvancedKnowledgeGraphVisualizer('./data/processed/knowledge_graph.gexf')
    visualizer.visualize()
