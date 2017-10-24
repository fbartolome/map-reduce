package ar.edu.itba.pod.client;

import ar.edu.itba.pod.mappers.InhabitantsByRegionMapper;
import ar.edu.itba.pod.mappers.WordOccurrencesMapper;
import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import ar.edu.itba.pod.reducers.InhabitantsByRegionReducerFactory;
import ar.edu.itba.pod.reducers.WordOccurrencesReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        logger.info("hazelcast Client Starting ...");
        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        IMap<Long,Person> map = client.getMap("people");
        Long count = new Long(0);
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Buenos Aires"));
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Chubut"));
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Chubut"));
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Buenos Aires"));
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Buenos Aires"));
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Catamarca"));
        map.put(count++, new Person(ActivityCondition.ECONOMICALLY_INACTIVE,1,"Hola","Buenos Aires"));

        JobTracker jobTracker = client.getJobTracker("tracker");
        Job<Long,Person> job = jobTracker.newJob(KeyValueSource.fromMap(map));
        try {
            ICompletableFuture<Map<String,Long>> future = job
                    .mapper(new InhabitantsByRegionMapper())
                    .reducer(new InhabitantsByRegionReducerFactory()).submit();
            Map<String,Long> response = future.get();
            for(Map.Entry<String,Long> entry : response.entrySet()){
                System.out.println(entry.getKey() + "\t\t" + entry.getValue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

}
