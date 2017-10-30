package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class HomesByRegionReducerFactory implements ReducerFactory<String, Integer, Integer>{


    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new HomesByRegionReducer();
    }

    private class HomesByRegionReducer extends Reducer<Integer, Integer>{

        Set<Integer> homeIds = new HashSet<>();

        @Override
        public void reduce(Integer homeId) {
            homeIds.add(homeId);
        }

        @Override
        public Integer finalizeReduce() {
            return homeIds.size();
        }
    }
}

