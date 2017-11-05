package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InhabitantsPerHouseholdByRegionReducerFactory implements ReducerFactory<String, Integer, Double> {

    @Override
    public Reducer<Integer, Double> newReducer(String s) {
        return new InhabitantsPerHouseholdByRegionReducer();
    }

    private class InhabitantsPerHouseholdByRegionReducer extends Reducer<Integer, Double>{
        private Set<Integer> set = new HashSet<>();
        private Double population = new Double(0);

        @Override
        public void reduce(Integer houseId) {
            set.add(houseId);
            population++;
        }

        @Override
        public Double finalizeReduce() {
            return BigDecimal.valueOf(population / set.size())
                .setScale(2, RoundingMode.HALF_UP).doubleValue();
        }
    }
}
