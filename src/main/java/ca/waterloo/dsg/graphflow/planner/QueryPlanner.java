package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.plan.Plan;
import ca.waterloo.dsg.graphflow.plan.Workers;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.EI;
import ca.waterloo.dsg.graphflow.plan.operator.extend.EI.CachingType;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashJoin;
import ca.waterloo.dsg.graphflow.plan.operator.jumpinglikejoin.JumpingLikeJoin;
import ca.waterloo.dsg.graphflow.plan.operator.jumpinglikejoin.JumpingLikeJoinExe;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink.SinkType;
import ca.waterloo.dsg.graphflow.planner.catalog.Catalog;
import ca.waterloo.dsg.graphflow.query.QueryEdge;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;
import ca.waterloo.dsg.graphflow.util.algorithm.FindArticulation;
import lombok.var;
import org.antlr.v4.runtime.misc.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Generates a {@link Plan}. The intersection cost (ICost) is used as a metric of the
 * optimization.
 */
public class QueryPlanner {

    protected static final Logger logger = LogManager.getLogger(QueryPlanner.class);

    private Map<Integer /* level: #(qVertices) covered [2, n] where n = #vertices in the query */,
            Map<String /* hash for query vertices covered */, List<Plan>>> subgraphPlans;

    QueryGraph queryGraph;
    int numVertices;
    int nextNumQVertices;
    protected Graph graph;
    protected Catalog catalog;
    boolean hasLimit;

    private int nextHashJoinID = 0;

    private Map<String /* encoding */,
            List<Pair<QueryGraph /*subgraph*/, Double /*selectivity*/>>> computedSelectivity;

    /**
     * Constructs a {@link QueryPlanner} object.
     *
     * @param queryGraph is the {@link QueryGraph} to evaluate.
     * @param catalog    is the catalog containing cost and selectivity stats of and intersections
     *                   for the graph.
     * @param graph      is the graph to evaluate the query on.
     */
    public QueryPlanner(QueryGraph queryGraph, Catalog catalog, Graph graph) {
        this.queryGraph = queryGraph;
        this.hasLimit = queryGraph.getLimit() > 0;
        this.catalog = catalog;
        this.graph = graph;
        this.subgraphPlans = new HashMap<>();
        this.numVertices = queryGraph.getNumVertices();
        this.computedSelectivity = new HashMap<>(1000 /*capacity*/);
    }

    /**
     * Returns based on the optimizer the 'best' {@link Plan} to evaluate a given
     * {@link QueryGraph}.
     *
     * @return The generated {@link Plan} to evaluate the input query graph.
     */
    public Plan plan() {
        if (numVertices == 2) {
            return new Plan(new Scan(queryGraph));
        }
        considerAllScanOperators();
        logger.debug("nextNumQVertices and numVertices is " + nextNumQVertices + " " + numVertices);
        while (nextNumQVertices <= numVertices) {
            considerAllNextQueryExtensions();
            nextNumQVertices++;
        }
        var key = subgraphPlans.get(numVertices).keySet().iterator().next();
        logger.debug("key is " + key);
        var bestPlan = getBestPlan(numVertices, key);
        // each operator added only sets its prev pointer (to reuse operator objects).
        // the picked plan needs to set the next pointer for each operator in the linear subplans.
        setNextPointers(bestPlan);
        if (hasLimit) {
            bestPlan.setSinkType(SinkType.LIMIT);
            bestPlan.setOutTuplesLimit(queryGraph.getLimit());
        }
        return bestPlan;
    }

    void setNextPointers(Plan bestPlan) {
        for (var lastOperator : bestPlan.getSubplans()) {
            var operator = lastOperator;
            while (null != operator.getPrev()) {
                operator.getPrev().setNext(operator);
                operator = operator.getPrev();
            }
        }
    }

    private void considerAllScanOperators() {
        nextNumQVertices = 2; /* level = 2 for edge scan */
        subgraphPlans.putIfAbsent(nextNumQVertices, new HashMap<>());
        for (var queryEdge : queryGraph.getEdges()) {
            var outSubgraph = new QueryGraph();
            outSubgraph.addEdge(queryEdge);
            var scan = new Scan(outSubgraph);
            var numEdges = getNumEdges(queryEdge);
            var queryPlan = new Plan(scan, numEdges);
            var queryPlans = new ArrayList<Plan>();
            queryPlans.add(queryPlan);
            subgraphPlans.get(nextNumQVertices).put(getKey(new String[]{
                    queryEdge.getFromVertex(), queryEdge.getToVertex()}), queryPlans);
        }
        nextNumQVertices = 3;
    }

    private void considerAllNextQueryExtensions() {
        subgraphPlans.putIfAbsent(nextNumQVertices, new HashMap<>());
        var prevNumQVertices = nextNumQVertices - 1;
        // prior considerate WCO
        for (var prevQueryPlans : subgraphPlans.get(prevNumQVertices).values()) {
            considerAllNextExtendOperators(prevQueryPlans);
        }
        // then reconsider Binary Join
        if (!hasLimit && nextNumQVertices >= 4) {
            for (var queryPlans : subgraphPlans.get(nextNumQVertices).values()) {
                var outSubgraph = queryPlans.get(0).getLastOperator().getOutSubgraph();
                considerAllNextHashJoinOperators(outSubgraph);
            }
        }
    }

    private void considerAllNextExtendOperators(List<Plan> prevQueryPlans) {
        var prevQVertices = prevQueryPlans.get(0).getLastOperator().getOutSubgraph().getQVertices();
        var toQVertices = queryGraph.getNeighbors(new HashSet<>(prevQVertices));
        for (String toQVertex : toQVertices) {
            for (var prevQueryPlan : prevQueryPlans) {
                Pair<String /* key */, Plan> newQueryPlan = getPlanWithNextExtend(
                        prevQueryPlan, toQVertex);
                subgraphPlans.get(nextNumQVertices).putIfAbsent(newQueryPlan.a, new ArrayList<>());
                subgraphPlans.get(nextNumQVertices).get(newQueryPlan.a).add(newQueryPlan.b);
            }
        }
    }

    Pair<String, Plan> getPlanWithNextExtend(Plan prevQueryPlan, String toQVertex) {
        var lastOperator = prevQueryPlan.getLastOperator();
        var inSubgraph = lastOperator.getOutSubgraph();
        var ALDs = new ArrayList<AdjListDescriptor>();
        var toType = queryGraph.getVertexType(toQVertex);
        var nextExtend = getNextEI(inSubgraph, toQVertex, ALDs, lastOperator);
        var lastPreviousRepeatedIndex = lastOperator.getLastRepeatedVertexIdx();
        nextExtend.initCaching(lastPreviousRepeatedIndex);

        var prevEstimatedNumOutTuples = prevQueryPlan.getEstimatedNumOutTuples();
        var estimatedSelectivity = getSelectivity(inSubgraph, nextExtend.getOutSubgraph(), ALDs,
                nextExtend.getOutSubgraph().getVertexType(toQVertex));
        double icost;
        if (nextExtend.getCachingType() == CachingType.NONE) {
            icost = prevEstimatedNumOutTuples * catalog.getICost(inSubgraph, ALDs,
                    nextExtend.getToType());
        } else {
            var outTuplesToProcess = prevEstimatedNumOutTuples;
            if (null != prevQueryPlan.getLastOperator().getPrev()) {
                var index = -1;
                var lastEstimatedNumOutTuplesForExtensionQVertex = -1.0;
                for (var ALD : ALDs) {
                    if (ALD.getVertexIdx() > index) {
                        lastEstimatedNumOutTuplesForExtensionQVertex = prevQueryPlan.
                                getQVertexToNumOutTuples().get(ALD.getFromQueryVertex());
                    }
                }
                outTuplesToProcess /= lastEstimatedNumOutTuplesForExtensionQVertex;
            }
            if (nextExtend.getCachingType() == CachingType.FULL_CACHING) {
                icost = outTuplesToProcess * catalog.getICost(inSubgraph, ALDs, toType) +
                        // added to make caching effect on cost more robust.
                        (prevEstimatedNumOutTuples - outTuplesToProcess) * estimatedSelectivity;
            } else { // cachingType == CachingType.PARTIAL_CACHINGuses
                var ALDsToCache = ALDs.stream()
                        .filter(ALD -> ALD.getVertexIdx() <= lastPreviousRepeatedIndex)
                        .collect(Collectors.toList());
                var ALDsToAlwaysIntersect = ALDs.stream()
                        .filter(ALD -> ALD.getVertexIdx() > lastPreviousRepeatedIndex)
                        .collect(Collectors.toList());
                var alwaysIntersectICost = prevEstimatedNumOutTuples * catalog.getICost(
                        inSubgraph, ALDsToAlwaysIntersect, toType);
                var cachedIntersectICost = outTuplesToProcess * catalog.getICost(
                        inSubgraph, ALDsToCache, toType);
                icost = prevEstimatedNumOutTuples * alwaysIntersectICost +
                        outTuplesToProcess * cachedIntersectICost +
                        // added to make caching effect on cost more robust.
                        (prevEstimatedNumOutTuples - outTuplesToProcess) * estimatedSelectivity;
            }
        }

        var estimatedICost = prevQueryPlan.getEstimatedICost() + icost;
        var estimatedNumOutTuples = prevEstimatedNumOutTuples * estimatedSelectivity;

        var qVertexToNumOutTuples = new HashMap<String, Double>();
        qVertexToNumOutTuples.putAll(prevQueryPlan.getQVertexToNumOutTuples());
        qVertexToNumOutTuples.put(nextExtend.getToQueryVertex(), estimatedNumOutTuples);

        var newQueryPlan = prevQueryPlan.shallowCopy();
        newQueryPlan.setEstimatedICost(estimatedICost);
        newQueryPlan.setEstimatedNumOutTuples(estimatedNumOutTuples);
        newQueryPlan.append(nextExtend);
        newQueryPlan.setQVertexToNumOutTuples(qVertexToNumOutTuples);
        return new Pair<>(getKey(nextExtend.getOutQVertexToIdxMap().keySet()), newQueryPlan);
    }

    private EI getNextEI(QueryGraph inSubgraph, String toQVertex, List<AdjListDescriptor> ALDs,
                         Operator lastOperator) {
        var outSubgraph = inSubgraph.copy();
        for (String fromQVertex : inSubgraph.getQVertices()) {
            if (queryGraph.containsQueryEdge(fromQVertex, toQVertex)) {
                // simple query graph so there is only 1 queryEdge, so get queryEdge at index '0'.
                var queryEdge = queryGraph.getEdge(fromQVertex, toQVertex);
                var index = lastOperator.getOutQVertexToIdxMap().get(fromQVertex);
                var direction = fromQVertex.equals(queryEdge.getFromVertex()) ?
                        Direction.Fwd : Direction.Bwd;
                var label = queryEdge.getLabel();
                ALDs.add(new AdjListDescriptor(fromQVertex, index, direction, label));
                outSubgraph.addEdge(queryEdge);
            }
        }
        var outputVariableIdxMap = new HashMap<String, Integer>();
        outputVariableIdxMap.putAll(lastOperator.getOutQVertexToIdxMap());
        outputVariableIdxMap.put(toQVertex, outputVariableIdxMap.size());
        return EI.make(toQVertex, queryGraph.getVertexType(toQVertex), ALDs, outSubgraph,
                inSubgraph, outputVariableIdxMap);
    }

    int getNumEdges(QueryEdge queryEdge) {
        var fromType = queryGraph.getVertexType(queryEdge.getFromVertex());
        var toType = queryGraph.getVertexType(queryEdge.getToVertex());
        var label = queryEdge.getLabel();
        return graph.getNumEdges(fromType, toType, label);
    }

    private double getSelectivity(QueryGraph inSubgraph, QueryGraph outSubgraph,
                                  List<AdjListDescriptor> ALDs, short toType) {
        double selectivity;

        if (computedSelectivity.containsKey(outSubgraph.getEncoding())) {
            for (var computedSelectivity : computedSelectivity.get(outSubgraph.getEncoding())) {
                if (computedSelectivity.a /* query graph */.isIsomorphicTo(outSubgraph)) {
                    return computedSelectivity.b;
                }
            }
        } else {
            computedSelectivity.put(outSubgraph.getEncoding(), new ArrayList<>());
        }
        selectivity = catalog.getSelectivity(inSubgraph, ALDs, toType);
        computedSelectivity.get(outSubgraph.getEncoding()).add(
                new Pair<>(outSubgraph, selectivity));
        return selectivity;
    }

    private void considerAllNextHashJoinOperators(QueryGraph outSubgraph) {
        var queryVertices = new ArrayList<>(outSubgraph.getQVertices());
        var minSize = 3;
        var maxSize = outSubgraph.getQVertices().size() - minSize;
        if (maxSize < minSize) {
            maxSize = minSize;
        }
        var it = IntStream.rangeClosed(minSize, maxSize).iterator();
        while (it.hasNext()) {
            var setSize = it.next(); // 从3到maxSize
            for (var key : subgraphPlans.get(setSize).keySet()) {
                var prevQueryPlan = getBestPlan(setSize, key);
                var prevQVertices = prevQueryPlan.getLastOperator().getOutSubgraph().getQVertices();
                if (SetUtils.isSubset(queryVertices, prevQVertices)) {
                    var otherSet = SetUtils.subtract(queryVertices, prevQVertices);
                    if (otherSet.size() == 1) {
                        continue;
                    }
                    var joinQVertices = getJoinQVertices(outSubgraph, prevQVertices, otherSet);
                    if (joinQVertices.size() != 1 ||
                            otherSet.size() + joinQVertices.size() > nextNumQVertices - 1) {
                        continue;
                    }
                    otherSet.addAll(joinQVertices);
                    var restKey = getKey(otherSet);
                    var restSize = otherSet.size();
                    if (subgraphPlans.get(restSize).containsKey(restKey)) {
                        var otherPrevOperator = getBestPlan(restSize, restKey);
                        considerHashJoinOperator(outSubgraph, queryVertices, prevQueryPlan,
                                otherPrevOperator, joinQVertices.size());
                    }
                }
            }
        }
    }

    private static List<String> getJoinQVertices(QueryGraph queryGraph, Collection<String> vertices,
                                                 List<String> otherVertices) {
        var joinQVertices = new HashSet<String>();
        for (var vertex : vertices) {
            for (var otherVertex : otherVertices) {
                if (queryGraph.containsQueryEdge(vertex, otherVertex)) {
                    joinQVertices.add(vertex);
                }
            }
        }
        return new ArrayList<>(joinQVertices);
    }


    private void considerHashJoinOperator(QueryGraph outSubgraph, List<String> queryVertices,
                                          Plan subplan, Plan otherSubplan, int numJoinQVertices) {
        var isPlanBuildSubplan =
                subplan.getEstimatedNumOutTuples() < otherSubplan.getEstimatedNumOutTuples();
        var buildSubplan = isPlanBuildSubplan ? subplan : otherSubplan;
        var probeSubplan = isPlanBuildSubplan ? otherSubplan : subplan;
        var buildCoef = numJoinQVertices == 1 ?
                Catalog.SINGLE_VERTEX_WEIGHT_BUILD_COEF : Catalog.MULTI_VERTEX_WEIGHT_BUILD_COEF;
        var probeCoef = numJoinQVertices == 1 ?
                Catalog.SINGLE_VERTEX_WEIGHT_PROBE_COEF : Catalog.MULTI_VERTEX_WEIGHT_PROBE_COEF;
        var icost = buildSubplan.getEstimatedICost() + probeSubplan.getEstimatedICost() +
                buildCoef * buildSubplan.getEstimatedNumOutTuples() +
                probeCoef * probeSubplan.getEstimatedNumOutTuples();

        var key = getKey(queryVertices);
        var currBestQueryPlan = getBestPlan(queryVertices.size(), key);
        if (currBestQueryPlan.getEstimatedICost() > icost) {
            var queryPlan = HashJoin.make(outSubgraph, buildSubplan, probeSubplan,
                    nextHashJoinID++);
            queryPlan.setEstimatedICost(icost);
            queryPlan.setEstimatedNumOutTuples(currBestQueryPlan.getEstimatedNumOutTuples());
            var vertexToNumOutTuples = new HashMap<String, Double>(
                    probeSubplan.getQVertexToNumOutTuples());
            for (var vertex : buildSubplan.getLastOperator().getOutSubgraph().getQVertices()) {
                var estimatedNumOutTuples = currBestQueryPlan.getEstimatedNumOutTuples();
                vertexToNumOutTuples.putIfAbsent(vertex, estimatedNumOutTuples);
            }
            queryPlan.setQVertexToNumOutTuples(vertexToNumOutTuples);

            var queryPlans = subgraphPlans.get(queryVertices.size()).get(key);
            queryPlans.clear();
            queryPlans.add(queryPlan);
        }
    }

    private Plan getBestPlan(int numQVertices, String key) {
        var possibleQueryPlans = subgraphPlans.get(numQVertices).get(key);
        var bestPlan = possibleQueryPlans.get(0);
        for (var possibleQueryPlan : possibleQueryPlans) {
            if (possibleQueryPlan.getEstimatedICost() < bestPlan.getEstimatedICost()) {
                bestPlan = possibleQueryPlan;
            }
        }
        return bestPlan;
    }

    private String getKey(Collection<String> queryVertices) {
        var queryVerticesArr = new String[queryVertices.size()];
        queryVertices.toArray(queryVerticesArr);
        return getKey(queryVerticesArr);
    }

    private String getKey(String[] queryVertices) {
        Arrays.sort(queryVertices);
        return Arrays.toString(queryVertices);
    }


    public Plan planWithJump() {
        switch (numVertices) {
            case 4:
                return Jump3();
            case 5:
                return Jump4();
            case 6:
                return Jump5();
            case 7:
                return Jump6();
            case 8:
                return Jump7();
            default:
                return plan();
        }
    }


	public Plan Jump3() {
        System.out.println("jump");
        
        var outSubgraph = new QueryGraph();
        outSubgraph.addEdge(queryGraph.getEdge("p1", "p2"));
        outSubgraph.addEdge(queryGraph.getEdge("p2", "p3"));
        outSubgraph.addEdge(queryGraph.getEdge("p3", "p4"));
        var jumpingTo3 = new JumpingLikeJoinExe(outSubgraph, graph);
        var plan = new Plan(jumpingTo3);
        return plan;
	}

    
	public Plan Jump4() {
        System.out.println("jump4");

       var outSubgraph = new QueryGraph();
       var queryEdge = queryGraph.getEdge("p1", "p2");
       outSubgraph.addEdge(queryEdge);
       var scan = new Scan(outSubgraph);
       var numEdges = getNumEdges(queryEdge);
       var plan = new Plan(scan, numEdges);
       plan = getPlanWithNextExtend(plan, "p3").b;

       var inSubgraph = new QueryGraph();
       inSubgraph.addEdge(queryGraph.getEdge("p1", "p2"));
       inSubgraph.addEdge(queryGraph.getEdge("p2", "p3"));

       outSubgraph = new QueryGraph();
       outSubgraph.addEdge(queryGraph.getEdge("p1", "p2"));
       outSubgraph.addEdge(queryGraph.getEdge("p2", "p3"));
       outSubgraph.addEdge(queryGraph.getEdge("p3", "p4"));
       outSubgraph.addEdge(queryGraph.getEdge("p4", "p5"));
       var jumpTo4 = new JumpingLikeJoin(outSubgraph, inSubgraph, graph, 2);
       plan.append(jumpTo4);

        return plan;
	}
	public Plan Jump5() {
        System.out.println("jump5");
        var outSubgraph = new QueryGraph();
        outSubgraph.addEdge(queryGraph.getEdge("p1", "p2"));
        outSubgraph.addEdge(queryGraph.getEdge("p2", "p3"));
        var jumpingTo2 = new JumpingLikeJoinExe(outSubgraph, graph);
        var plan = new Plan(jumpingTo2);
        var inSubgraph = outSubgraph;
        outSubgraph = new QueryGraph();
        outSubgraph.addEdge(queryGraph.getEdge("p1", "p2"));
        outSubgraph.addEdge(queryGraph.getEdge("p2", "p3"));
        outSubgraph.addEdge(queryGraph.getEdge("p3", "p4"));
        outSubgraph.addEdge(queryGraph.getEdge("p4", "p5"));
        outSubgraph.addEdge(queryGraph.getEdge("p5", "p6"));
        var jumpingTo5 = new JumpingLikeJoin(outSubgraph, inSubgraph, graph, 3);
        plan.append(jumpingTo5);
        
        return plan;
	}
	public Plan Jump6() {
        System.out.println("jump6");
       var outSubgraph = new QueryGraph();
       var queryEdge = queryGraph.getEdge("p1", "p2");
       outSubgraph.addEdge(queryEdge);
       queryEdge = queryGraph.getEdge("p2", "p3");
       outSubgraph.addEdge(queryEdge);
       queryEdge = queryGraph.getEdge("p3", "p4");
       outSubgraph.addEdge(queryEdge);
       var jumpingLikeJoin2Build = new JumpingLikeJoinExe(outSubgraph, graph);
       List<Operator> preBuild = new ArrayList<>();
       preBuild.add(jumpingLikeJoin2Build);

       outSubgraph = new QueryGraph();
       queryEdge = queryGraph.getEdge("p4", "p5");
       outSubgraph.addEdge(queryEdge);
       queryEdge = queryGraph.getEdge("p5", "p6");
       outSubgraph.addEdge(queryEdge);
       queryEdge = queryGraph.getEdge("p6", "p7");
       outSubgraph.addEdge(queryEdge);
       var jumpingLikeJoin2Probe = new JumpingLikeJoinExe(outSubgraph, graph);
       List<Operator> preProbe = new ArrayList<>();
       preProbe.add(jumpingLikeJoin2Probe);
       return new Plan(HashJoin.make(queryGraph, preBuild, preProbe, 0));
	}

	public Plan Jump7() {
        System.out.println("jump7");

    // 7边 = 3边Jump、4边jump + HashJoin
       var outSubgraph = new QueryGraph();
       var queryEdge = queryGraph.getEdge("p1", "p2");
       outSubgraph.addEdge(queryEdge);
       queryEdge = queryGraph.getEdge("p2", "p3");
       outSubgraph.addEdge(queryEdge);
       queryEdge = queryGraph.getEdge("p3", "p4");
       outSubgraph.addEdge(queryEdge);
       var jumpTo3 = new JumpingLikeJoinExe(outSubgraph, graph);
       List<Operator> preBuild = new ArrayList<>();
       preBuild.add(jumpTo3);

       outSubgraph = new QueryGraph();
       queryEdge = queryGraph.getEdge("p4", "p5");
       outSubgraph.addEdge(queryEdge);
       var scan = new Scan(outSubgraph);
       var numEdges = getNumEdges(queryEdge);
       var plan = new Plan(scan, numEdges);
       plan = getPlanWithNextExtend(plan, "p6").b;

       var inSubgraph = new QueryGraph();
       inSubgraph.addEdge(queryGraph.getEdge("p4", "p5"));
       inSubgraph.addEdge(queryGraph.getEdge("p5", "p6"));

       outSubgraph = new QueryGraph();
       outSubgraph.addEdge(queryGraph.getEdge("p4", "p5"));
       outSubgraph.addEdge(queryGraph.getEdge("p5", "p6"));
       outSubgraph.addEdge(queryGraph.getEdge("p6", "p7"));
       outSubgraph.addEdge(queryGraph.getEdge("p7", "p8"));
       var jumpTo4 = new JumpingLikeJoin(outSubgraph, inSubgraph, graph, 2);
       plan.append(jumpTo4);
       List<Operator> preProbe = new ArrayList<>();
       preProbe.add(jumpTo4);
       return new Plan(HashJoin.make(queryGraph, preBuild, preProbe, 0));
        
	}

    public Plan getArticulationJoinPlan() {
        FindArticulation findArticulation = new FindArticulation();
        var articulations = findArticulation.dfsTarjan(queryGraph);

        String[] points = articulations.toArray(new String[0]);
        var subgraphVertices = findArticulation.getSubgraph(queryGraph, points[0]);
        var subgraphVerticesKey = getKey(subgraphVertices);

        var otherSubgraphVertices = SetUtils.subtract(queryGraph.getQVertices(), subgraphVertices);
        otherSubgraphVertices.add(points[0]);
        var otherSubgraphVerticesKey = getKey(otherSubgraphVertices);

        var maxNumVertices = Math.max(subgraphVertices.size(), otherSubgraphVertices.size());
        considerAllScanOperators();
        logger.debug("nextNumQVertices and numVertices is " + nextNumQVertices + " " + numVertices);
        while (nextNumQVertices <= maxNumVertices) {
            considerAllNextQueryExtensions();
            nextNumQVertices++;
        }


        var plan1 = getBestPlan(subgraphVertices.size(), subgraphVerticesKey);
        List<Operator> preProbe = new ArrayList<>();
        preProbe.add(plan1.getLastOperator());

        var plan2 = getBestPlan(otherSubgraphVertices.size(), otherSubgraphVerticesKey);
        List<Operator> preBuild = new ArrayList<>();
        preBuild.add(plan2.getLastOperator());

        return new Plan(HashJoin.make(queryGraph, preProbe, preBuild, 0));
    }
}
