package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InhabitantsPerHouseholdByRegionMapper implements Mapper<Long,Pair<Integer,String>,String,Integer> {

  @Override
  public void map(Long aLong, Pair<Integer,String> pair,
      Context<String, Integer> context) {
    context.emit(pair.getValue(),pair.getKey());
  }
}
