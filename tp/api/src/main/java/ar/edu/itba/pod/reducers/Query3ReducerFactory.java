package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLong;

public class Query3ReducerFactory implements
        ReducerFactory<Character,Pair<Integer,Integer>,Double> {

    @Override
    public Reducer<Pair<Integer,Integer>, Double> newReducer(Character s) {
        return new Query3Reducer();
    }

    private class Query3Reducer extends Reducer<Pair<Integer,Integer>,Double>{

        private AtomicLong employed = new AtomicLong(0);
        private AtomicLong unemployed = new AtomicLong(0);

        @Override
        public void reduce(Pair<Integer,Integer> pair) {
            unemployed.addAndGet(pair.getKey());
            employed.addAndGet(pair.getValue());
        }

        @Override
        public Double finalizeReduce() {
            return BigDecimal.valueOf(unemployed.doubleValue() / (employed.doubleValue() + unemployed.doubleValue()))
                    .setScale(2, RoundingMode.FLOOR).doubleValue();
        }
    }
}
