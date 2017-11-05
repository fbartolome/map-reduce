package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class MostInhabitedProvinceDeptsMapper implements Mapper<Long,String,String,Integer> {

    @Override
    public void map(Long aLong, String department, Context<String, Integer> context) {
        context.emit(department,1);
    }
}
