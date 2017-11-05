package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InhabitantsByRegionMapper implements Mapper<Long,String,String,Integer> {

  @Override
  public void map(Long aLong, String region, Context<String, Integer> context) {
    context.emit(region,1);
  }
}
