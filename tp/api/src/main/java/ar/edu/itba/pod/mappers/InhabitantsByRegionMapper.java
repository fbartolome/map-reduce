package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InhabitantsByRegionMapper implements Mapper<Long,Character,Character,Integer> {

  @Override
  public void map(Long aLong, Character region, Context<Character, Integer> context) {
    context.emit(region,1);
  }
}
