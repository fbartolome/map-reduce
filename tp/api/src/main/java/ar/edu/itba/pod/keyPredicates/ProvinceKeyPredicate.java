package ar.edu.itba.pod.keyPredicates;

import ar.edu.itba.pod.model.Person;
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
        final Map<Long,Person> map = hazelcastInstance.getMap(mapName);
        final Person person = map.get(id);
        return person.getProvinceName().equalsIgnoreCase(province);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
