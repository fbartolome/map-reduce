package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class UnemploymentIndexByRegionMapper implements Mapper<Long,Person,String,ActivityCondition> {

  @Override
  public void map(Long aLong, Person person, Context<String, ActivityCondition> context) {
    if(person.getActivityCondition() == ActivityCondition.EMPLOYED ||
        person.getActivityCondition() == ActivityCondition.UNEMPLOYED){
      context.emit(person.getRegion(),person.getActivityCondition());
    }
  }
}
