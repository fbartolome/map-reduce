package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class InhabitantsByRegionReducerFactory implements ReducerFactory<String,Integer,Long> {

  @Override
  public Reducer<Integer, Long> newReducer(String s) {
    return new InhabitantsByRegion();
  }

  private class InhabitantsByRegion extends Reducer<Integer,Long> {

    Long count = new Long(0);

    @Override
    public void reduce(Integer integer) {
      count += integer;
    }

    @Override
    public Long finalizeReduce() {
      return count;
    }
  }
}
