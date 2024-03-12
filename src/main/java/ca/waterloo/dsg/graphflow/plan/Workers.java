package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator.LimitExceededException;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.Build;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashTable;
import ca.waterloo.dsg.graphflow.plan.operator.jumpinglikejoin.JumpingLikeJoin;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanBlocking;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanBlocking.VertexIdxLimits;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Triple;
import lombok.Getter;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Query plan workers execute a query plan in parallel given a number of threads.
 */
public class Workers {

    protected static final Logger logger = LogManager.getLogger(Workers.class);

    private Plan[] queryPlans;
    private Thread[][] workers;
    private int numThreads = 1;

    private JumpingLikeJoin jumpingLikeJoin;

    @Getter
    private double elapsedTime = 0;
    private long intersectionCost = 0;
    private long numIntermediateTuples = 0;
    private long numOutTuples = 0;
    transient private List<Triple<String /* name */,
            Long /* i-cost */, Long /* prefixes size */>> operatorMetrics;

    /**
     * Constructs a {@link Workers} object.
     *
     * @param queryPlan  is the query plan to execute.
     * @param numThreads is the number of threads to use executing the query.
     */
    public Workers(Plan queryPlan, int numThreads) {
        queryPlans = new Plan[numThreads];
        if (numThreads == 1) {
            queryPlans[0] = queryPlan;
        } else { // numThreads > 1
            for (int i = 0; i < numThreads; i++) {
                queryPlans[i] = queryPlan.copy(true /* isThreadSafe */);
            }
            this.numThreads = numThreads;
            var numSubplans = queryPlans[0].getSubplans().size();
            workers = new Thread[numSubplans][numThreads];
            for (var i = 0; i < queryPlans.length; i++) {
                var subplans = queryPlans[i].getSubplans();
                for (var subplanId = 0; subplanId < numSubplans; subplanId++) {
                    var operator = subplans.get(subplanId);
                    Runnable runnable = () -> {
                        try {
                            operator.execute();
                        } catch (LimitExceededException e) {
                        }
                    };
                    workers[subplanId][i] = new Thread(runnable);
                }
            }
            for (var i = 0; i < numSubplans; i++) {
                var globalVertexIdxLimits = new VertexIdxLimits();
                for (var plan : queryPlans) {
                    var lastOperator = plan.subplans.get(i);
                    var operator = lastOperator;
                    while (null != operator.getPrev()) {
                        operator = operator.getPrev();
                    }
                    if (operator instanceof ScanBlocking) {
                        ((ScanBlocking) operator).setGlobalVerticesIdxLimits(globalVertexIdxLimits);
                    }
                }
            }
        }
    }

    public void init(Graph graph, KeyStore store, short label) {
        // delete cycle
        /*RemLoop remLoop = new RemLoop(queryPlans[queryPlans.length - 1].getLastOperator().getOutSubgraph());
        queryPlans[queryPlans.length - 1].append(remLoop);*/
        for (var queryPlan : queryPlans) {
            queryPlan.init(graph, store);
        }
        // run jumping-like-wco plans directly，theb we no need to invoke scan operator
        // jumpingLikeJoin = new JumpingLikeJoin(graph, label); // invoke jumping
        var numBuildOperators = queryPlans[0].getSubplans().size() - 1;
        for (var buildIdx = 0; buildIdx < numBuildOperators; buildIdx++) {
            var ID = ((Build) queryPlans[0].getSubplans().get(buildIdx)).getID();
            var hashTables = new HashTable[numThreads];
            for (var i = 0; i < queryPlans.length; i++) {
                hashTables[i] = ((Build) queryPlans[i].getSubplans().get(buildIdx)).getHashTable();
            }
            for (var queryPlan : queryPlans) {
                queryPlan.setProbeHashTables(ID, hashTables);
            }
        }

    }

    public void execute() throws InterruptedException {
        if (queryPlans.length == 1) {
            if (jumpingLikeJoin == null) {
                queryPlans[0].execute();
                elapsedTime = queryPlans[0].getElapsedTime();
            } else {
                // var startTime = System.nanoTime();
                // var edge3ByTable = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge3ByTable.size();

                // // 6(4(2+1)+1)+1
                // System.out.println("jump 6");
                // var startTime = System.nanoTime();
                // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // var edge4 = jumpingLikeJoin.intersect(edge2);
                // var edge6 = jumpingLikeJoin.intersect(edge4);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge6.size();

                // // 7(5(3+1)+1)+1
                // System.out.println("jump 7");
                // var startTime = System.nanoTime();
                // var edge3 = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // var edge5 = jumpingLikeJoin.intersect(edge3);
                // var edge7 = jumpingLikeJoin.intersect(edge5);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge7.size();

                // // 8=3+4（2）/6(4(2+1)+1)+1
                // System.out.println("jump 8");
                // var startTime = System.nanoTime();
                // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // var edge3 = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // var edge4 = jumpingLikeJoin.intersect(edge2);
                // jumpingLikeJoin.buildSubTable(edge3);
                // var edge8 = jumpingLikeJoin.intersect(edge4,edge3);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge8.size();
                // // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // // var edge3 = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // // jumpingLikeJoin.buildSubTable(edge2);
                // // var edge6 = jumpingLikeJoin.intersect(edge3,edge2);
                // // var edge8 = jumpingLikeJoin.intersect(edge6);
                // // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // // elapsedTime = endTime;
                // // numOutTuples = edge8.size();
                // // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // // var edge4 = jumpingLikeJoin.intersect(edge2);
                // // var edge6 = jumpingLikeJoin.intersect(edge4);
                // // var edge8 = jumpingLikeJoin.intersect(edge6);
                // // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // // elapsedTime = endTime;
                // // numOutTuples = edge8.size();

                // 9=4+4（2），7（3+3）+1，7（5（3+1）+1）+1
                // System.out.println("jump 9");
                // var startTime = System.nanoTime();
                // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // var edge4 = jumpingLikeJoin.intersect(edge2);
                // jumpingLikeJoin.buildSubTable(edge4);
                // var edge9 = jumpingLikeJoin.intersect(edge4,edge4);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge9.size();

                // var edge3 = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // jumpingLikeJoin.buildSubTable(edge3);
                // var edge7 = jumpingLikeJoin.intersect(edge3,edge3);
                // var edge9 = jumpingLikeJoin.intersect(edge7);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge9.size();

                // var edge3 = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // var edge5 = jumpingLikeJoin.intersect(edge3);
                // var edge7 = jumpingLikeJoin.intersect(edge5);
                // var edge9 = jumpingLikeJoin.intersect(edge7);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge9.size();

                // // 10=4+5，8（6（4（2+1）+1）+1）+1
                // System.out.println("jump 10");
                // var startTime = System.nanoTime();
                // // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // // var edge4 = jumpingLikeJoin.intersect(edge2);
                // // jumpingLikeJoin.buildSubTable(edge2);
                // // var edge5 = jumpingLikeJoin.intersect(edge2,edge2);
                // // jumpingLikeJoin.buildSubTable(edge4);
                // // var edge10 = jumpingLikeJoin.intersect(edge5,edge4);
                // // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // // elapsedTime = endTime;
                // // numOutTuples = edge10.size();

                // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // var edge4 = jumpingLikeJoin.intersect(edge2);
                // var edge6 = jumpingLikeJoin.intersect(edge4);
                // var edge8 = jumpingLikeJoin.intersect(edge6);
                // var edge10 = jumpingLikeJoin.intersect(edge8);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge10.size();

                // // 11=9(7,5,3)+1
                // System.out.println("jump 11");
                // var startTime = System.nanoTime();
                // var edge3 = jumpingLikeJoin.getEdge3ByFwdAdjList();
                // // jumpingLikeJoin.buildSubTable(edge3);
                // // var edge7 = jumpingLikeJoin.intersect(edge3,edge3);
                // var edge5 = jumpingLikeJoin.intersect(edge3);
                // var edge7 = jumpingLikeJoin.intersect(edge5);
                // var edge9 = jumpingLikeJoin.intersect(edge7);
                // var edge11 = jumpingLikeJoin.intersect(edge9);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge11.size();

                // // 12=10（8，6，4，2）+1
                // System.out.println("jump 12");
                // var startTime = System.nanoTime();
                // var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                // var edge4 = jumpingLikeJoin.intersect(edge2);
                // var edge6 = jumpingLikeJoin.intersect(edge4);
                // var edge8 = jumpingLikeJoin.intersect(edge6);
                // var edge10 = jumpingLikeJoin.intersect(edge8);
                // var edge12 = jumpingLikeJoin.intersect(edge10);
                // var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                // elapsedTime = endTime;
                // numOutTuples = edge12.size();


                // 24=10+11
                System.out.println("jump 24");
                var startTime = System.nanoTime();
                var edge2 = jumpingLikeJoin.getEdge2ByFwdAdjList();
                var edge4 = jumpingLikeJoin.intersect(edge2);
                var edge6 = jumpingLikeJoin.intersect(edge4);
                var edge8 = jumpingLikeJoin.intersect(edge6);
                var edge10 = jumpingLikeJoin.intersect(edge8);
                var edge12 = jumpingLikeJoin.intersect(edge10);
                var edge14 = jumpingLikeJoin.intersect(edge12);
                var edge16 = jumpingLikeJoin.intersect(edge14);
                var edge18 = jumpingLikeJoin.intersect(edge16);
                var edge20 = jumpingLikeJoin.intersect(edge18);
                var edge22 = jumpingLikeJoin.intersect(edge20);
                var edge24 = jumpingLikeJoin.intersect(edge22);
                var endTime = IOUtils.getElapsedTimeInMillis(startTime);
                elapsedTime = endTime;
                numOutTuples = edge24.size();
            }
        } else {
            var beginTime = System.nanoTime();
            for (var subplanWorkers : workers) {
                for (int j = 0; j < queryPlans.length; j++) {
                    subplanWorkers[j].start();
                }
                for (int j = 0; j < queryPlans.length; j++) {
                    subplanWorkers[j].join();
                }
            }
            elapsedTime = IOUtils.getElapsedTimeInMillis(beginTime);
        }
    }

    /**
     * @return The stats as a one line comma separated CSV  one line row for logging.
     */
    public String getOutputLog() {
        if (jumpingLikeJoin != null) {
            var czyStr = new StringJoiner(",");
            czyStr.add(String.format("%.4f", elapsedTime));
            czyStr.add(String.format("%d", numOutTuples));
            czyStr.add(String.format("%d", -1));
            czyStr.add("Jumping Like Join");
            return czyStr.toString() + '\n';
        }
        // ------------------------------------------------------
        if (queryPlans.length == 1) {
            return queryPlans[0].getOutputLog();
        }
        if (null == operatorMetrics) {
            operatorMetrics = new ArrayList<>();
            for (var queryPlan : queryPlans) {
                queryPlan.setStats();
            }
            aggregateOutput();
        }
        var strJoiner = new StringJoiner(",");
        strJoiner.add(String.format("%.4f", elapsedTime));
        strJoiner.add(String.format("%d", numOutTuples));
        strJoiner.add(String.format("%d", numIntermediateTuples));
        strJoiner.add(String.format("%d", intersectionCost));
        for (var operatorMetric : operatorMetrics) {
            strJoiner.add(String.format("%s", operatorMetric.a));     /* operator name */
            /*
            if (!operatorMetric.a.contains("PROBE") && !operatorMetric.a.contains("HASH") &&
                !operatorMetric.a.contains("SCAN")) {
                strJoiner.add(String.format("%d", operatorMetric.b)); /* i-cost *
            }
            if (!operatorMetric.a.contains("HASH")) {
                strJoiner.add(String.format("%d", operatorMetric.c)); /* output tuples size *
            }
            */
        }
        return strJoiner.toString() + "\n";
    }

    private void aggregateOutput() {
        operatorMetrics = new ArrayList<>();
        for (var queryPlan : queryPlans) {
            intersectionCost += queryPlan.getIcost();
            numIntermediateTuples += queryPlan.getNumIntermediateTuples();
            numOutTuples += queryPlan.getSink().getNumOutTuples();
        }
        var queryPlan = queryPlans[0];
        for (var metric : queryPlan.getOperatorMetrics()) {
            operatorMetrics.add(new Triple<>(metric.a, metric.b, metric.c));
        }
        for (int i = 1; i < queryPlans.length; i++) {
            for (int j = 0; j < operatorMetrics.size(); j++) {
                operatorMetrics.get(j).b += queryPlans[i].getOperatorMetrics().get(j).b;
                operatorMetrics.get(j).c += queryPlans[i].getOperatorMetrics().get(j).c;
            }
        }
    }
}
