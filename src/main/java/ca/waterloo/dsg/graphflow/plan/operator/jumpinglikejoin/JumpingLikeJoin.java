package ca.waterloo.dsg.graphflow.plan.operator.jumpinglikejoin;

import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.SortedAdjList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JumpingLikeJoin {
    private SortedAdjList[] fwdAdjList;
    private short label;

    private Map<Integer, List<Integer>> subTable; // record the vertex joined and the vertices followed the joined vertex

    public JumpingLikeJoin(Graph graph, short label) {
        this.label = label;
        this.fwdAdjList = graph.getFwdAdjLists();
        this.subTable = new HashMap<>();
    }

    public void buildSubTable(List<int[]> temps) {
        for (int[] temp : temps) {
            subTable.putIfAbsent(temp[0], new ArrayList<>());
            List<Integer> l = subTable.get(temp[0]);
            l.add(temp[1]);
        }
    }

    /**
     * Jumping like join two tables.
     *
     * @param t1 主表
     * @param t2 被hash的表
     * @return 连接后的结果
     */
    public List<int[]> intersect(List<int[]> t1, List<int[]> t2) {
        buildSubTable(t2);
        List<int[]> res = new ArrayList<>();

        for (int[] r : t1) {
            int toVertexStartIdx = fwdAdjList[r[1]].getLabelOrTypeOffsets()[label];
            int toVertexEndIdx = fwdAdjList[r[1]].getLabelOrTypeOffsets()[label + 1];
            for (int i = toVertexStartIdx; i < toVertexEndIdx; i++) {
                int neighborId = fwdAdjList[r[1]].getNeighbourId(i);
                List<Integer> l = subTable.get(neighborId);
                if (l != null) {
                    for (Integer t2EndId : l) {
                        int[] row = new int[2];
                        row[0] = r[0];
                        row[1] = t2EndId;
                        res.add(row);
                    }
                }
            }
        }
        return res;
    }

    public List<int[]> getEdge3ByFwdAdjList() {
        List<int[]> res = new ArrayList<>();
        // t1ID -表t1-> t1NeighborID -表edge-> edgeNeighborID -表t2-> t2NeighborID
        for (int t1ID = 0; t1ID < fwdAdjList.length; t1ID++) {
            int t1StartIdx = fwdAdjList[t1ID].getLabelOrTypeOffsets()[label];
            int t1EndIdx = fwdAdjList[t1ID].getLabelOrTypeOffsets()[label + 1];
            for (int i = t1StartIdx; i < t1EndIdx; i++) {
                int t1NeighborID = fwdAdjList[t1ID].getNeighbourId(i);
                int edgeStartIdx = fwdAdjList[t1NeighborID].getLabelOrTypeOffsets()[label];
                int edgeEndIdx = fwdAdjList[t1NeighborID].getLabelOrTypeOffsets()[label + 1];
                for (int j = edgeStartIdx; j < edgeEndIdx; j++) {
                    int edgeNeighborID = fwdAdjList[t1NeighborID].getNeighbourId(j);
                    int t2StartIdx = fwdAdjList[edgeNeighborID].getLabelOrTypeOffsets()[label];
                    int t2EndIdx = fwdAdjList[edgeNeighborID].getLabelOrTypeOffsets()[label + 1];
                    for (int k = t2StartIdx; k < t2EndIdx; k++) {
                        int t2NeighborID = fwdAdjList[edgeNeighborID].getNeighbourId(k);
                        int[] tmpRes = new int[2];
                        tmpRes[0] = t1ID;
                        tmpRes[1] = t2NeighborID;
                        res.add(tmpRes);
                    }
                }
            }
        }
        return res;
    }
}
