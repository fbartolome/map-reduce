package ar.edu.itba.pod.keyPredicates;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;

public class ActivityConditionKeyPredicate implements KeyPredicate<Long>, HazelcastInstanceAware {

    private transient HazelcastInstance hazelcastInstance;

    public ActivityConditionKeyPredicate(String mapName) {

    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public boolean evaluate(Long id) {
        final Map<Long,Pair<Character, ActivityCondition>> map = hazelcastInstance.getMap("map3");
        final Pair<Character, ActivityCondition> person = map.get(id);
        return person.getValue().equals(ActivityCondition.EMPLOYED) ||
                person.getValue().equals(ActivityCondition.UNEMPLOYED);
    }
}
