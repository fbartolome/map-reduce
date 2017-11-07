package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.concurrent.atomic.AtomicLong;

public class CountReducerFactory<K> implements ReducerFactory<K,Integer,Long> {
    @Override
    public Reducer<Integer, Long> newReducer(K k) {
        return new CountReducer();
    }

    private class CountReducer extends Reducer<Integer, Long> {
        private AtomicLong count = new AtomicLong(0);

        @Override
        public void reduce(Integer integer) {
            count.addAndGet(integer);
        }

        @Override
        public Long finalizeReduce() {
            return count.longValue();
        }
    }
}
