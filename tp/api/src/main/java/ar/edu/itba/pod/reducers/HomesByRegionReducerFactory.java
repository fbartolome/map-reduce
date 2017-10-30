package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.concurrent.ConcurrentSkipListSet;

public class HomesByRegionReducerFactory implements ReducerFactory<String, Integer, Integer>{

    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new HomesByRegionReducer();
    }

    private class HomesByRegionReducer extends Reducer<Integer, Integer>{

        ConcurrentSkipListSet homeIds = new ConcurrentSkipListSet();

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

