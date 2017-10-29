package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class InhabitantsByRegionReducerFactory implements ReducerFactory<String,Long,Long> {

  @Override
  public Reducer<Long, Long> newReducer(String s) {
    return new InhabitantsByRegionReducer();
  }

  private class InhabitantsByRegionReducer extends Reducer<Long,Long> {

    private Long count = new Long(0);

    @Override
    public void reduce(Long aLong) {
      //TODO: remove
      System.out.println(aLong);
      count += aLong;
    }

    @Override
    public Long finalizeReduce() {
      //TODO: remove
      System.out.println("-------");
      System.out.println(count);
      System.out.println("-------");
      return count;
    }
  }
}
