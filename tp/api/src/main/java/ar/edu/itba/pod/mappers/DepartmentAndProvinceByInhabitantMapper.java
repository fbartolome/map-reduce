package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DepartmentAndProvinceByInhabitantMapper implements Mapper<Long, Pair<String, Character>, String, Integer>  {

    @Override
    public void map(Long aLong, Pair<String, Character> pair, Context<String, Integer> context) {
        context.emit(pair.getKey(), 1);
    }
}
