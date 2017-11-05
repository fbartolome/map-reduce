package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CountReducerFactory<K> implements ReducerFactory<K,Integer,Long> {
    @Override
    public Reducer<Integer, Long> newReducer(K k) {
        return new CountReducer();
    }

    private class CountReducer extends Reducer<Integer, Long> {
        private volatile long count = 0;

        @Override
        public void reduce(Integer integer) {
            count += integer;
        }

        @Override
        public Long finalizeReduce() {
            return count;
        }
    }
}
