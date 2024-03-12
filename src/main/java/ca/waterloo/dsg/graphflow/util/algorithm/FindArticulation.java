package ca.waterloo.dsg.graphflow.util.algorithm;

import ca.waterloo.dsg.graphflow.query.QueryGraph;
import lombok.var;

import java.lang.reflect.Array;
import java.util.*;

public class FindArticulation {
    enum Color {
        WHITE, GRAY;
    }

    private class Node {
        public String vertex;
        public int d;
        public Color color;
        public String pi;
        public int low;

        public Node() {
        }

        public Node(String vertex) {
            this.vertex = vertex;
            this.d = 0;
            this.color = Color.WHITE;
            this.pi = "";
        }
    }

    private Map<String, Node> nodes;
    private int time;
    private Set<String> articulations;

    public FindArticulation() {

    }

    private void dfs(QueryGraph queryGraph, Node u) {
        time++;
        u.d = time;
        u.low = time;
        u.color = Color.GRAY;
        int childNum = 0;
        var neighbors = queryGraph.getNeighbors(u.vertex);
        for (String v : neighbors) {
            Node v_node = nodes.get(v);
            if (v_node.color == Color.WHITE) {
                childNum++;
                v_node.pi = u.vertex;
                dfs(queryGraph, v_node);
                u.low = Math.min(u.low, v_node.low);
                if (u.pi.contentEquals("") && childNum > 1)
                    articulations.add(u.vertex);
                else if (!u.pi.contentEquals("") && v_node.low >= u.d)
                    articulations.add(u.vertex);
            } else if (!u.pi.contentEquals(v_node.vertex))
                u.low = Math.min(u.low, v_node.d);
        }
    }

    public Set<String> dfsTarjan(QueryGraph queryGraph) {
        var vertices = queryGraph.getQVertices();
        nodes = new HashMap<>();
        articulations = new HashSet<String>();
        time = 0;
        for (String v : vertices)
            nodes.put(v, new Node(v));
        for (Node node : nodes.values())
            if (node.color == Color.WHITE) {
                dfs(queryGraph, node);
            }


        return articulations;
    }

    private void dfsSubgraph(QueryGraph queryGraph, Node node, Node fromNode) {
        node.color = Color.GRAY;
        var neighbors = queryGraph.getNeighbors(node.vertex);
        if (node == fromNode)
            dfsSubgraph(queryGraph, nodes.get(neighbors.get(0)), fromNode);
        else
            for (var v : neighbors) {
                var v_node = nodes.get(v);
                if (v_node.color == Color.WHITE)
                    dfsSubgraph(queryGraph, v_node, fromNode);
            }
    }

    public List<String> getSubgraph(QueryGraph queryGraph, String vertex) {
        for (var node : nodes.values())
            node.color = Color.WHITE;
        var vnode = nodes.get(vertex);
        dfsSubgraph(queryGraph, vnode, vnode);

        var subgraphVertices = new ArrayList<String>();
        for (var node : nodes.values())
            if (node.color == Color.GRAY)
                subgraphVertices.add(node.vertex);
        return subgraphVertices;
    }
}
