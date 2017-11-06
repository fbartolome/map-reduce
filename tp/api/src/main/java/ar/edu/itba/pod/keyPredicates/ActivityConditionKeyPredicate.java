package ar.edu.itba.pod.keyPredicates;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;

public class ActivityConditionKeyPredicate implements KeyPredicate<Long>, HazelcastInstanceAware {

    private transient HazelcastInstance hazelcastInstance;
    private final String mapName;

    public ActivityConditionKeyPredicate(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public boolean evaluate(Long id) {
        final Map<Long,Person> map = hazelcastInstance.getMap(mapName);
        final Person person = map.get(id);
        return person.getActivityCondition().equals(ActivityCondition.EMPLOYED) ||
                person.getActivityCondition().equals(ActivityCondition.UNEMPLOYED);
    }
}
