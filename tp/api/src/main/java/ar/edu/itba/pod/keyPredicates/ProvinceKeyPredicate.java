package ar.edu.itba.pod.keyPredicates;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.KeyPredicate;

import java.util.Map;

public class ProvinceKeyPredicate implements KeyPredicate<Long>, HazelcastInstanceAware {

    private transient HazelcastInstance hazelcastInstance;
    private final String mapName;
    private final String province;

    public ProvinceKeyPredicate(String mapName, String province) {
        this.mapName = mapName;
        this.province = province;
    }


    @Override
    public boolean evaluate(Long id) {
        //IMap<Long, Pair<Character, ActivityCondition>>
        final Map<Long,String> map = hazelcastInstance.getMap(mapName);
        final String dept = map.get(id);
        return dept.equalsIgnoreCase(province);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
