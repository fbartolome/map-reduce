package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InhabitantsByRegionMapper implements Mapper<Long,String,String,Long> {

  @Override
  public void map(Long aLong, String region, Context<String, Long> context) {
    context.emit(region,new Long(1));
  }
}
