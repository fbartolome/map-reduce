package ar.edu.itba.pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AddCombinerFactory<T,N extends Number> implements CombinerFactory<T,N,N> {

  private final N resetValue;

  public AddCombinerFactory(N resetValue) {
    this.resetValue = resetValue;
  }

  @Override
  public Combiner<N, N> newCombiner(T t) {
    return new AddCombiner();
  }

  private final Number addNumbers(Number a, Number b) {
    if(a instanceof Double || b instanceof Double) {
      return new Double(a.doubleValue() + b.doubleValue());
    } else if(a instanceof Float || b instanceof Float) {
      return new Float(a.floatValue() + b.floatValue());
    } else if(a instanceof Long || b instanceof Long) {
      return new Long(a.longValue() + b.longValue());
    } else {
      return new Integer(a.intValue() + b.intValue());
    }
  }

  private class AddCombiner extends Combiner<N, N> {
    //TODO: synchronize?
    private N sum = resetValue;

    @Override
    public void combine(N t) {
      sum = (N) addNumbers(sum,t);
    }

    @Override
    public N finalizeChunk() {
      return sum;
    }

    @Override
    public void reset(){
      sum = resetValue;
    }
  }
}
