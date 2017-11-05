package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class UnitMapper<K,S> implements Mapper<K,S,S,Integer> {

    @Override
    public void map(K k, S s, Context<S, Integer> context) {
        context.emit(s,1);
    }
}
