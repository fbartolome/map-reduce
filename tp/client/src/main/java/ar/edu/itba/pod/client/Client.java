package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.OrderByCollator;
import ar.edu.itba.pod.mappers.HomesByRegionMapper;
import ar.edu.itba.pod.model.Person;
import ar.edu.itba.pod.reducers.HomesByRegionReducerFactory;
import ar.edu.itba.pod.utils.CSVReader;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {

        logger.info("hazelcast Client Starting ...");

        final ClientConfig ccfg = new ClientConfig();

        //TODO deshardocdear esta lista - PREGUNTAR estas addresses que serian?
        List<String> addresses = new ArrayList<>();
        addresses.add("10.17.65.164:5701");
        addresses.add("127.0.0.1:5701");
        ccfg.getNetworkConfig().setAddresses(addresses);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        //TODO sacar del CSV
        IMap<Long,Person> map = client.getMap("people");
        final AtomicLong count = new AtomicLong(0);

        CSVReader.readCSV("./client/src/main/resources/census100.csv").stream().forEach(p -> map.put(count.getAndIncrement(), p));

        JobTracker jobTracker = client.getJobTracker("tracker");
        Job<Long,Person> job = jobTracker.newJob(KeyValueSource.fromMap(map));
        try {
            ICompletableFuture<List<Entry<String,Integer>>> future = job
                    .mapper(new HomesByRegionMapper())
//                    .combiner(new AddCombinerFactory<>(new Long(0)))
                    .reducer(new HomesByRegionReducerFactory())
                    .submit(new OrderByCollator<>(false,false));
            List<Entry<String,Integer>> response = future.get();
            for(Map.Entry<String,Integer> entry : response){
                System.out.println(entry.getKey() + "\t\t" + entry.getValue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

}


