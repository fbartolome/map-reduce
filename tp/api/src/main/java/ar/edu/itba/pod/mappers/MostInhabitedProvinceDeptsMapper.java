package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class MostInhabitedProvinceDeptsMapper implements Mapper<Long,Person,String,Long> {

    @Override
    public void map(Long aLong, Person person, Context<String, Long> context) {
        context.emit(person.getDepartmentName(),new Long(1));
    }
}
