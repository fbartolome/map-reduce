package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.ActivityCondition;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class UnemploymentByRegionReducerFactory implements
    ReducerFactory<String,ActivityCondition,Double> {

  @Override
  public Reducer<ActivityCondition, Double> newReducer(String s) {
      return new UnemploymentByRegionReducer();
  }

  private class UnemploymentByRegionReducer extends Reducer<ActivityCondition,Double>{

    private volatile long employed = 0;
    private volatile long unemployed = 0;

    @Override
    public void reduce(ActivityCondition activityCondition) {
      if(activityCondition == ActivityCondition.EMPLOYED){
        employed++;
      } else if(activityCondition == ActivityCondition.UNEMPLOYED){
        unemployed++;
      }
    }

    @Override
    public Double finalizeReduce() {
      return BigDecimal.valueOf(Double.valueOf(unemployed) / (employed + unemployed))
          .setScale(2, RoundingMode.HALF_DOWN).doubleValue();
    }
  }
}
