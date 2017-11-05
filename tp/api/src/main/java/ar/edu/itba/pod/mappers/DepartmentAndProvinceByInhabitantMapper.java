package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import javafx.util.Pair;

public class DepartmentAndProvinceByInhabitantMapper implements Mapper<Long, Pair<String, String>, String, String>  {

    @Override
    public void map(Long aLong, Pair<String, String> stringStringPair, Context<String, String> context) {
        context.emit(stringStringPair.getKey(), stringStringPair.getValue());
    }
}
