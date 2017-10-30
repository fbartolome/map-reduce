package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class MostInhabitedProvinceDeptsMapper implements Mapper<Long,Person,String,Long> {

    private final String province;

    public MostInhabitedProvinceDeptsMapper(String province) {
        this.province = province;
    }

    @Override
    public void map(Long aLong, Person person, Context<String, Long> context) {
        if(person.getProvinceName().equalsIgnoreCase(province)){
            context.emit(person.getDepartmentName(),new Long(1));
        }
    }
}
