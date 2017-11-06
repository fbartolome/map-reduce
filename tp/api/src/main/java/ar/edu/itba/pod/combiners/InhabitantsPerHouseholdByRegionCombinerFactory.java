package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class InhabitantsPerHouseholdByRegionCombinerFactory<T> implements
        CombinerFactory<T,Integer,Pair<Set<Integer>,Integer>>{
    @Override
    public Combiner<Integer, Pair<Set<Integer>, Integer>> newCombiner(T t) {
        return new InhabitantsPerHouseholdByRegionCombiner();
    }

    private class InhabitantsPerHouseholdByRegionCombiner extends Combiner<Integer, Pair<Set<Integer>, Integer>> {

        private Set<Integer> set = new ConcurrentSkipListSet();
        private AtomicInteger count = new AtomicInteger(0);

        @Override
        public void combine(Integer integer) {
            count.incrementAndGet();
            set.add(integer);
        }

        @Override
        public Pair<Set<Integer>, Integer> finalizeChunk() {
            return new Pair(set,count.intValue());
        }
    }
}
