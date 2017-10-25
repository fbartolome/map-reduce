package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class InhabitantsByRegionReducerFactory implements ReducerFactory<String,Integer,Long> {

  @Override
  public Reducer<Integer, Long> newReducer(String s) {
    return new InhabitantsByRegionReducer();
  }

  private class InhabitantsByRegionReducer extends Reducer<Integer,Long> {

    private Long count = new Long(0);

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
