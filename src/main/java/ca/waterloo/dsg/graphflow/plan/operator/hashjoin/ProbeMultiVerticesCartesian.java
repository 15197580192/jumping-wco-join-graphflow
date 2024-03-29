package ca.waterloo.dsg.graphflow.plan.operator.hashjoin;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashTable.BlockInfo;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ProbeMultiVerticesCartesian extends ProbeMultiVertices implements Serializable {

    private BlockInfo otherBlockInfo;
    private int highestVertexId;

    /**
     * @see ProbeMultiVertices#ProbeMultiVertices(QueryGraph, QueryGraph, List, int, int[], int[],
     * int, int, Map)
     */
    ProbeMultiVerticesCartesian(QueryGraph outSubgraph, QueryGraph inSubgraph,
        List<String> joinQVertices, int probeHashIdx, int[] probeIndices, int[] buildIndices,
        int hashedTupleLen, int probeTupleLen, Map<String, Integer> outQVertexToIdxMap) {
        super(outSubgraph, inSubgraph, joinQVertices, probeHashIdx, probeIndices, buildIndices,
            hashedTupleLen, probeTupleLen, outQVertexToIdxMap);
        this.name = "CARTESIAN " + this.name;
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        if (null == this.probeTuple) {
            highestVertexId = graph.getHighestVertexId();
            otherBlockInfo = new BlockInfo();
        }
        super.init(probeTuple, graph, store);
    }

    /**
     * @see Operator#execute()
     */
    @Override
    public void execute() throws LimitExceededException {
        for (var aHashVertex = 0; aHashVertex <= highestVertexId; aHashVertex++) {
            probeTuple[hashedTupleLen] = aHashVertex;
            for (var hashTable : hashTables) {
                var aLastChunkIdx = hashTable.numChunks[aHashVertex];
                var aPrevFirstVertex = -1;
                for (var aChunkIdx = 0; aChunkIdx < aLastChunkIdx; aChunkIdx++) {
                    hashTable.getBlockAndOffsets(aHashVertex, aChunkIdx, otherBlockInfo);
                    for (var anOffset = otherBlockInfo.startOffset;
                             anOffset < otherBlockInfo.endOffset  ;) {
                        if (hashedTupleLen == 2) {
                            var firstVertex = otherBlockInfo.block[anOffset++];
                            if (aPrevFirstVertex != firstVertex) {
                                probeTuple[0] = firstVertex;
                                aPrevFirstVertex = firstVertex;
                            }
                            probeTuple[1] = otherBlockInfo.block[anOffset++];
                        } else {
                            for (int k = 0; k < hashedTupleLen; k++) {
                                probeTuple[k] = otherBlockInfo.block[anOffset++];
                            }
                        }
                        super/* ProbeMultiVertices */.processNewTuple();
                    }
                }
            }
        }
    }

    /**
     * @see Operator#isSameAs(Operator)
     */
    public boolean isSameAs(Operator operator) {
        return this == operator || (operator instanceof ProbeMultiVerticesCartesian &&
            inSubgraph.isIsomorphicTo(operator.getInSubgraph()) &&
            outSubgraph.isIsomorphicTo(operator.getOutSubgraph())
        );
    }

    /**
     * @see Operator#copy(boolean)
     */
    public ProbeMultiVerticesCartesian copy(boolean isThreadSafe) {
        var probe = new ProbeMultiVerticesCartesian(outSubgraph, inSubgraph, joinQVertices,
            probeHashIdx, probeIndices, buildIndices, hashedTupleLen, probeTupleLen,
            outQVertexToIdxMap);
        probe.setID(ID);
        return probe;
    }
}
