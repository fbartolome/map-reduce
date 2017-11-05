package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import javafx.util.Pair;

public class DepartmentAndProvinceByInhabitantMapper implements Mapper<Long, Pair<String, String>, String, Integer>  {

    @Override
    public void map(Long aLong, Pair<String, String> pair, Context<String, Integer> context) {
        context.emit(pair.getKey(), 1);
    }
}
