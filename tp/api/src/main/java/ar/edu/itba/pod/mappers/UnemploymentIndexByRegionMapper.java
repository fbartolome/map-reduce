package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.ActivityCondition;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import javafx.util.Pair;

public class UnemploymentIndexByRegionMapper implements Mapper<Long,Pair<ActivityCondition, String>,String,ActivityCondition> {

  @Override
  public void map(Long aLong, Pair<ActivityCondition, String> activityConditionStringPair, Context<String, ActivityCondition> context) {
    context.emit(activityConditionStringPair.getValue(), activityConditionStringPair.getKey());
  }
}
