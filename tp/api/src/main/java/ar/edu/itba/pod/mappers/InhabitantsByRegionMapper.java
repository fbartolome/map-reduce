package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InhabitantsByRegionMapper implements Mapper<Long,Person,String,Integer> {

  @Override
  public void map(Long aLong, Person person, Context<String, Integer> context) {
    context.emit(person.getRegion(),1);
  }
}
