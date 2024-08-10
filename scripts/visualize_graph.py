import networkx as nx
import plotly.graph_objs as go
from networkx.algorithms.community import greedy_modularity_communities

class AdvancedKnowledgeGraphVisualizer:
    def __init__(self, graph_path: str):
        """
        Initializes the AdvancedKnowledgeGraphVisualizer with a path to the .gexf graph file.
        :param graph_path: Path to the .gexf file containing the graph data.
        """
        self.graph = self._load_graph(graph_path)
        self.pos = self._compute_layout()
        self.communities = self._detect_communities()

    def _load_graph(self, graph_path: str) -> nx.Graph:
        """
        Loads the knowledge graph from a .gexf file.
        :param graph_path: Path to the .gexf file.
        :return: A NetworkX Graph object.
        """
        try:
            graph = nx.read_gexf(graph_path)
            print(f"Graph loaded successfully with {len(graph.nodes())} nodes and {len(graph.edges())} edges.")
            return graph
        except Exception as e:
            print(f"Failed to load graph: {e}")
            raise

    def _compute_layout(self) -> dict:
        """
        Computes the layout for the graph visualization using a force-directed layout.
        :return: A dictionary with node positions.
        """
        try:
            layout = nx.spring_layout(self.graph, k=0.5, iterations=100)
            print("Graph layout computed successfully.")
            return layout
        except Exception as e:
            print(f"Failed to compute layout: {e}")
            raise

    def _detect_communities(self):
        """
        Detects communities within the graph using the Louvain method.
        :return: A list of communities.
        """
        communities = list(greedy_modularity_communities(self.graph))
        print(f"Detected {len(communities)} communities.")
        return communities

    def _create_edge_trace(self) -> go.Scatter:
        """
        Creates a Plotly Scatter trace for graph edges.
        :return: A Plotly Scatter object for the edges.
        """
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

        print("Edge trace created.")
        return edge_trace

    def _create_node_trace(self) -> go.Scatter:
        """
        Creates a Plotly Scatter trace for graph nodes.
        :return: A Plotly Scatter object for the nodes.
        """
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
                    title='Community',
                    xanchor='left',
                    titleside='right'
                ),
                line_width=2))

        for node in self.graph.nodes():
            x, y = self.pos[node]
            node_trace['x'] += tuple([x])
            node_trace['y'] += tuple([y])
            node_trace['text'] += tuple([f"{node} (Degree: {self.graph.degree[node]})"])

            # Determine community for color
            node_community = self._get_community(node)
            node_trace['marker']['color'] += tuple([node_community])

            # Set node size by degree
            node_trace['marker']['size'] += tuple([5 + 3 * self.graph.degree(node)])

        print("Node trace created.")
        return node_trace

    def _get_community(self, node):
        """
        Returns the community index for a given node.
        :param node: The node for which to determine the community.
        :return: The index of the community the node belongs to.
        """
        for i, community in enumerate(self.communities):
            if node in community:
                return i
        return -1

    def visualize(self, title: str = "Community-Based Knowledge Graph Visualization") -> None:
        """
        Visualizes the knowledge graph using Plotly.
        :param title: The title of the graph visualization.
        """
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

        print("Displaying graph visualization.")
        fig.show()

# Example usage
if __name__ == "__main__":
    visualizer = AdvancedKnowledgeGraphVisualizer('./data/processed/knowledge_graph.gexf')
    visualizer.visualize()
