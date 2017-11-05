package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class MostInhabitedProvinceDeptsMapper implements Mapper<Long,String,String,Long> {

    @Override
    public void map(Long aLong, String department, Context<String, Long> context) {
        context.emit(department,new Long(1));
    }
}
