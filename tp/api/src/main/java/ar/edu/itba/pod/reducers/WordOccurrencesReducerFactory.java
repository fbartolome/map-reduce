package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;

public class WordOccurrencesReducerFactory implements ReducerFactory<String,Long,Long> {

    public WordOccurrencesReducerFactory() {
    }

    @Override
    public Reducer<Long, Long> newReducer(String s) {
        return new WordOccurrencesReducer();
    }

    private class WordOccurrencesReducer extends Reducer<Long, Long> {
        Long count = new Long(0);

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
