package ar.edu.itba.pod.client;

import ar.edu.itba.pod.mappers.WordOccurrencesMapper;
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

        IMap<String,String> map = client.getMap("libros");
        map.put("Dracula","hola soy dracula hola no soy dracula hola soy la señora de dracula hola soy la seniora de dracula iras a Anglos muy proximamente");
        map.put("Pepito","Al temploooo");
        map.put("Jorge","Soy jorge");
        map.put("Judio","Dinero dinero");
        map.put("Barto","Me la como");

        JobTracker jobTracker = client.getJobTracker("tracker");
        Job<String,String> job = jobTracker.newJob(KeyValueSource.fromMap(map));
        try {
            ICompletableFuture<Map<String,Long>> future = job
                    .mapper(new WordOccurrencesMapper("Dracula"))
                    .reducer(new WordOccurrencesReducerFactory()).submit();
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
