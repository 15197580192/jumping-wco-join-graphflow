package ca.waterloo.dsg.graphflow.plan.operator.jumpinglikejoin;

import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;

public class JumpingLikeJoinExe extends JumpingLikeJoin {
    public JumpingLikeJoinExe(Graph graph, short label) {
        super(graph, label);
    }

    public JumpingLikeJoinExe(QueryGraph outSubgraph, Graph graph) {
        super(outSubgraph, graph);
    }

    @Override
    public void execute() throws LimitExceededException {
        jumpByFwd();
    }

    private void jumpByFwd() throws LimitExceededException {
        for (int t1ID = 0; t1ID < fwdAdjList.length; t1ID++) {
            probeTuple[0] = t1ID;
            int t1StartIdx = fwdAdjList[t1ID].getLabelOrTypeOffsets()[label];
            int t1EndIdx = fwdAdjList[t1ID].getLabelOrTypeOffsets()[label + 1];
            for (int i = t1StartIdx; i < t1EndIdx; i++) {
                int t1NeighborID = fwdAdjList[t1ID].getNeighbourId(i);
                probeTuple[1] = t1NeighborID;
                int edgeStartIdx = fwdAdjList[t1NeighborID].getLabelOrTypeOffsets()[label];
                int edgeEndIdx = fwdAdjList[t1NeighborID].getLabelOrTypeOffsets()[label + 1];
                for (int j = edgeStartIdx; j < edgeEndIdx; j++) {
                    int edgeNeighborID = fwdAdjList[t1NeighborID].getNeighbourId(j);
                    probeTuple[2] = edgeNeighborID;
                    int t2StartIdx = fwdAdjList[edgeNeighborID].getLabelOrTypeOffsets()[label];
                    int t2EndIdx = fwdAdjList[edgeNeighborID].getLabelOrTypeOffsets()[label + 1];
                    for (int k = t2StartIdx; k < t2EndIdx; k++) {
                        int t2NeighborID = fwdAdjList[edgeNeighborID].getNeighbourId(k);
                        probeTuple[3] = t2NeighborID;
                        numOutTuples++;
                        next[0].processNewTuple();
                    }
                }
            }
        }
    }

    private void jumpByBwd() throws LimitExceededException {
        for (int t1ID = 0; t1ID < bwdAdjList.length; t1ID++) {
            probeTuple[0] = t1ID;
            int t1StartIdx = bwdAdjList[t1ID].getLabelOrTypeOffsets()[label];
            int t1EndIdx = bwdAdjList[t1ID].getLabelOrTypeOffsets()[label + 1];
            for (int i = t1StartIdx; i < t1EndIdx; i++) {
                int t1NeighborID = bwdAdjList[t1ID].getNeighbourId(i);
                probeTuple[1] = t1NeighborID;
                int edgeStartIdx = bwdAdjList[t1NeighborID].getLabelOrTypeOffsets()[label];
                int edgeEndIdx = bwdAdjList[t1NeighborID].getLabelOrTypeOffsets()[label + 1];
                for (int j = edgeStartIdx; j < edgeEndIdx; j++) {
                    int edgeNeighborID = bwdAdjList[t1NeighborID].getNeighbourId(j);
                    probeTuple[2] = edgeNeighborID;
                    int t2StartIdx = bwdAdjList[edgeNeighborID].getLabelOrTypeOffsets()[label];
                    int t2EndIdx = bwdAdjList[edgeNeighborID].getLabelOrTypeOffsets()[label + 1];
                    for (int k = t2StartIdx; k < t2EndIdx; k++) {
                        int t2NeighborID = bwdAdjList[edgeNeighborID].getNeighbourId(k);
                        probeTuple[3] = t2NeighborID;
                        numOutTuples++;
                        next[0].processNewTuple();
                    }
                }
            }
        }
    }

}
