package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class HomesByRegionMapper implements Mapper<Integer,String,String,Integer> {

    @Override
    public void map(Integer homeId, String region, Context<String, Integer> context) {
        context.emit(region, 1);
    }
}
