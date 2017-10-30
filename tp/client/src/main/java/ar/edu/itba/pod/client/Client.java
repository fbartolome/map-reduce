package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.OrderByCollator;
import ar.edu.itba.pod.collators.TopAndOrderByCollator;
import ar.edu.itba.pod.keyPredicates.ActivityConditionKeyPredicate;
import ar.edu.itba.pod.keyPredicates.ProvinceKeyPredicate;
import ar.edu.itba.pod.mappers.HomesByRegionMapper;
import ar.edu.itba.pod.mappers.MostInhabitedProvinceDeptsMapper;
import ar.edu.itba.pod.mappers.UnemploymentIndexByRegionMapper;
import ar.edu.itba.pod.model.Person;
import ar.edu.itba.pod.reducers.CountReducerFactory;
import ar.edu.itba.pod.reducers.HomesByRegionReducerFactory;
import ar.edu.itba.pod.reducers.UnemploymentByRegionReducerFactory;
import ar.edu.itba.pod.utils.CLIParser;
import ar.edu.itba.pod.utils.CSVReader;
import ar.edu.itba.pod.utils.ConsoleArguments;
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

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.exit;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {

        CLIParser cli = new CLIParser(args);

        ConsoleArguments arguments = cli.parse();

        logger.info("hazelcast Client Starting ...");

        final ClientConfig ccfg = new ClientConfig();

        ccfg.getNetworkConfig().setAddresses(Arrays.asList(arguments.getIps()));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        //TODO sacar del CSV
        IMap<Long,Person> map = client.getMap("people");
        final AtomicLong count = new AtomicLong(0);

        //"./client/src/main/resources/census100.csv"
        CSVReader.readCSV(arguments.getInputPath()).stream().forEach(p -> map.put(count.getAndIncrement(), p));

        JobTracker jobTracker = client.getJobTracker("tracker");
        Job<Long,Person> job = jobTracker.newJob(KeyValueSource.fromMap(map));
        try {
            ICompletableFuture<List<Entry<String,Double>>> future = job
                    .keyPredicate(new ActivityConditionKeyPredicate("people"))
                    .mapper(new UnemploymentIndexByRegionMapper())
                    .reducer(new UnemploymentByRegionReducerFactory())
                    .submit(new TopAndOrderByCollator<>(20,false,false));
            List<Entry<String,Double>> response = future.get();
            for(Map.Entry<String,Double> entry : response){
                System.out.println(entry.getKey() + "\t\t" + entry.getValue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

}


