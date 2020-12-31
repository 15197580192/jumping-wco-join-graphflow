package ca.waterloo.dsg.graphflow.plan.operator.jumpinglikejoin;

import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.SortedAdjList;
import lombok.Getter;
import lombok.var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JumpingLikeJoin implements Runnable {
    private SortedAdjList[] fwdAdjList;
    private short label;

    @Getter
    private List<int[]> edge3;

    private Map<Integer, List<Integer>> subTable; // record the vertex joined and the vertices followed the joined vertex

    public JumpingLikeJoin(Graph graph, short label) {
        this.label = label;
        this.fwdAdjList = graph.getFwdAdjLists();
        this.subTable = new HashMap<>();
        this.edge3 = null;
    }

    public void buildSubTable(List<int[]> temps) {
        subTable.clear();
        for (int[] temp : temps) {
            subTable.putIfAbsent(temp[0], new ArrayList<>());
            List<Integer> l = subTable.get(temp[0]);
            l.add(temp[1]);
        }
    }

    /**
     * Jumping like join 表和一边的表
     *
     * @param t 和一边连接的表
     * @return 连接结果
     */
    public List<int[]> intersect(List<int[]> t) {
        List<int[]> res = new ArrayList<>();
        // r[0] -> r[1] -> r1NeighborId -> destinationID
        for (int[] r : t) {
            int r1StartIdx = fwdAdjList[r[1]].getLabelOrTypeOffsets()[label];
            int r1EndIdx = fwdAdjList[r[1]].getLabelOrTypeOffsets()[label + 1];
            for (int i = r1StartIdx; i < r1EndIdx; i++) {
                int r1NeighborId = fwdAdjList[r[1]].getNeighbourId(i);
                int edge1StartIdx = fwdAdjList[r1NeighborId].getLabelOrTypeOffsets()[label];
                int edg1EndIdx = fwdAdjList[r1NeighborId].getLabelOrTypeOffsets()[label + 1];
                for (int j = edge1StartIdx; j < edg1EndIdx; j++) {
                    int destinationID = fwdAdjList[r1NeighborId].getNeighbourId(j);
                    int[] row = new int[2];
                    row[0] = r[0];
                    row[1] = destinationID;
                    res.add(row);
                }
            }
        }
        return res;
    }

    /**
     * Jumping like join two tables.
     *
     * @param t1 主表
     * @param t2 被hash的表
     * @return 连接后的结果
     */
    public List<int[]> intersect(List<int[]> t1, List<int[]> t2) {
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

    @Override
    public void run() {
        edge3 = getEdge3ByFwdAdjList();
    }

    public List<int[]> testEdge3toEdge6() {
        List<int[]> res = new ArrayList<>();
        var edge3 = getEdge3ByFwdAdjList();
        buildSubTable(edge3);
        for (var e : edge3) {
            var l = subTable.getOrDefault(e[1], new ArrayList<>());
            for (var i : l)
                res.add(new int[]{e[0], i});
        }
        return res;
    }

}
