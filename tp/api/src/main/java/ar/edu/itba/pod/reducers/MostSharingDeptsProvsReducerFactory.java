package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.HashSet;
import java.util.Set;

public class MostSharingDeptsProvsReducerFactory implements ReducerFactory<String,String,Integer> {

  @Override
  public Reducer<String, Integer> newReducer(String s) {
    return new MostSharingDeptsProvsReducer();
  }

  private class MostSharingDeptsProvsReducer extends Reducer<String, Integer> {

    private int count = 0;
    private Set<String> set = new HashSet<>();

    @Override
    public synchronized void reduce(String department) {
      if(set.contains(department)){
        count++;
      } else {
        set.add(department);
      }
    }

    @Override
    public Integer finalizeReduce() {
      return count;
    }
  }
}
