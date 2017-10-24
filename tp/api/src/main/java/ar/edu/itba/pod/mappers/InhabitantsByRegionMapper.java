package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InhabitantsByRegionMapper implements Mapper<String,Integer,String,Integer> {

  @Override
  public void map(String s, Integer integer, Context<String, Integer> context) {
    context.emit(s,1);
  }
}
