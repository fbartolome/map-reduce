package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.HashSet;
import java.util.Set;

public class MostSharingDeptsProvsReducerFactory implements ReducerFactory<Pair<Character,Character>,String,Long> {

  @Override
  public Reducer<String, Long> newReducer(Pair<Character, Character> s) {
    return new MostSharingDeptsProvsReducer();
  }

  private class MostSharingDeptsProvsReducer extends Reducer<String,Long> {

    private Long count = 0L;
    private Set<String> set = new HashSet<String>();

    @Override
    public synchronized void reduce(String department) {
      if(set.contains(department)){
        count++;
      } else {
        set.add(department);
      }
    }

    @Override
    public Long finalizeReduce() {
      return count;
    }
  }
}
