package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class UnemploymentByRegionCombinerFactory<K> implements CombinerFactory<K,ActivityCondition,Pair<Integer,Integer>> {
    @Override
    public Combiner<ActivityCondition, Pair<Integer, Integer>> newCombiner(K k) {
        return new UnemploymentByRegionCombiner();
    }

    private class UnemploymentByRegionCombiner extends Combiner<ActivityCondition, Pair<Integer, Integer>> {

        private Pair<Integer,Integer> pair = new Pair(0,0);

        @Override
        synchronized public void combine(ActivityCondition ac) {
            if(ac.equals(ActivityCondition.UNEMPLOYED)){
                pair.setKey(pair.getKey() + 1);
            } else if (ac.equals(ActivityCondition.EMPLOYED)) {
                pair.setValue(pair.getValue() + 1);
            }
        }

        @Override
        public Pair<Integer, Integer> finalizeChunk() {
            return pair;
        }
    }
}
