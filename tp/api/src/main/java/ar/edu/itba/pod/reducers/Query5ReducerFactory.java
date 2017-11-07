package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class Query5ReducerFactory implements ReducerFactory<Character,Pair<Set<Integer>,Integer>,Double> {

    @Override
    public Reducer<Pair<Set<Integer>, Integer>, Double> newReducer(Character character) {
        return new Query5Reducer();
    }

    private class Query5Reducer extends Reducer<Pair<Set<Integer>, Integer>, Double> {

        Set<Integer> set = new ConcurrentSkipListSet();
        AtomicInteger population = new AtomicInteger(0);

        @Override
        public void reduce(Pair<Set<Integer>, Integer> pair) {
            set.addAll(pair.getKey());
            population.addAndGet(pair.getValue());
        }

        @Override
        public Double finalizeReduce() {
            return BigDecimal.valueOf(population.doubleValue() / set.size())
                    .setScale(2, RoundingMode.FLOOR).doubleValue();
        }
    }
}
