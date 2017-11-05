package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import javafx.util.Pair;

public class KeyValueMapper <K,S,V> implements Mapper<K,Pair<S,V>,S,V> {

    @Override
    public void map(K k, Pair<S,V> pair, Context<S, V> context) {
        context.emit(pair.getKey(),pair.getValue());
    }
}