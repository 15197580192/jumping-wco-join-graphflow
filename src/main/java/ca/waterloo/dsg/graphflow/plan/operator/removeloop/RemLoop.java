package ca.waterloo.dsg.graphflow.plan.operator.removeloop;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;

import java.util.HashMap;
import java.util.HashSet;

public class RemLoop extends Operator {

    private HashSet<Integer> set;

    public RemLoop(QueryGraph outSubgraph) {
        super(outSubgraph, null);
    }

    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        this.probeTuple = probeTuple;
        set = new HashSet<>();
        name = "Remove Loop.";
    }

    @Override
    public void processNewTuple() throws LimitExceededException {
        set.clear();
        for (int id : probeTuple) {
            if (set.contains(id))
                return;
            set.add(id);
        }
        numOutTuples++;
    }
}
