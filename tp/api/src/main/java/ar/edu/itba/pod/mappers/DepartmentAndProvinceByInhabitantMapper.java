package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import javafx.util.Pair;

public class DepartmentAndProvinceByInhabitantMapper implements Mapper<Long,Person, String, String>  {

    @Override
    public void map(Long aLong, Person person, Context<String ,String> context) {
        context.emit(person.getDepartmentName(), person.getProvinceName());
    }
}
