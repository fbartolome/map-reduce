package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CountReducerFactory<K> implements ReducerFactory<K,Long,Long> {
    @Override
    public Reducer<Long, Long> newReducer(K k) {
        return new CountReducer();
    }

    private class CountReducer extends Reducer<Long, Long> {
        private volatile long count = 0;

        @Override
        public void reduce(Long aLong) {
            count += aLong;
        }

        @Override
        public Long finalizeReduce() {
            return count;
        }
    }
}
