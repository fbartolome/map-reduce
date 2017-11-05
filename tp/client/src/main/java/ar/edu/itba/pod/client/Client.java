package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.utils.*;
import com.google.common.base.Stopwatch;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static String mapName = "people";

    public static void main(String[] args) {
        logger.info("hazelcast Client Starting ...");


        CLIParser cli = new CLIParser(args);
        ConsoleArguments arguments = cli.parse();
        final ClientConfig ccfg = new ClientConfig();
        ccfg.getNetworkConfig().setAddresses(Arrays.asList(arguments.getIps()));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        //TODO: no necesariamente es un mapa
//        IMap<Long,Person> map = client.getMap(mapName);
        final AtomicLong count = new AtomicLong(0);

        //"./client/src/main/resources/census100.csv"
//        CSVReader.readCSV(arguments.getInputPath()).stream().forEach(p -> map.put(count.getAndIncrement(), p));


        JobTracker jobTracker = client.getJobTracker("tracker");
        //TODO: los jobs no se hacen siempre igual, hay que ponerlos en cada case
//        Job<Long,Person> job = jobTracker.newJob(KeyValueSource.fromMap(map));
//        Job <String,String> job7 = null;

        Stopwatch timer = Stopwatch.createUnstarted();
        try {
            PrintWriter writer = new PrintWriter(arguments.getOutPath(), "UTF-8");
            Query query = null;
            switch (arguments.getQueryNumber()) {

                case 1:
                    logger.info("Creating local map with data");
                    final IMap<Long,String> map1 = client.getMap(mapName);
                    map1.clear();
                    Map<Long,String> query1Map = new HashMap();
                    timer.start();
                    CSVReader.getRegions(arguments.getInputPath()).stream()
                        .forEach(r -> query1Map.put(count.getAndIncrement(),r));
                    logger.info("Reading data took: " + timer);
                    timer.stop().reset();
                    timer.start();
                    logger.info("Start loading remote data");
                    map1.putAll(query1Map);
                    logger.info("Finished loading remote data");
                    query = new QueryManager.FirstQuery();
                    Job <Long,String> job1 = jobTracker.newJob(KeyValueSource.fromMap(map1));
                    query.output(writer, query.getFuture(job1).get());
                    logger.info("The query took: " + timer);
                    timer.stop().reset();
                    logger.info("Finished writing output");
                    break;

                case 2:
                    logger.info("Creating local map with data");
                    HashMap<Long, String> query2Map = new HashMap<>();
                    timer.start();
                    CSVReader.departmentInProv(arguments.getInputPath(), arguments.getProvince())
                            .stream()
                            .forEach(d -> query2Map.put(count.getAndIncrement(), d));
                    IMap<Long,String> map2 = client.getMap("departments");
                    map2.clear();
                    logger.info("Start loading remote data");
                    map2.putAll(query2Map);
                    logger.info("Reading data took: " + timer);
                    timer.stop().reset();
                    timer.start();
                    logger.debug("map size " + map2.size());
                    logger.info("Finished loading remote data");
                    Job<Long, String> job2 = jobTracker.newJob(KeyValueSource.fromMap(map2));
                    query = new QueryManager.SecondQuery(arguments.getAmount(), arguments.getProvince());
                    query.output(writer, query.getFuture(job2).get());
                    logger.info("The query took: " + timer);
                    timer.stop().reset();
                    logger.info("Finished writing output");
                    break;

                case 3:
                    logger.info("Creating local map with data");
                    HashMap<Long, Pair<ActivityCondition, String>> auxMap = new HashMap<>();
                    CSVReader.getConditionByRegion(arguments.getInputPath())
                            .stream()
                            .forEach(p -> auxMap.put(count.getAndIncrement(), p));
                    IMap<Long, Pair<ActivityCondition, String>> map3 = client.getMap("activityConditions");
                    logger.info("Start loading remote data");
                    map3.putAll(auxMap);
                    logger.info("Finished loading remote data");
                    Job<Long, Pair<ActivityCondition, String>> job3 = jobTracker.newJob(KeyValueSource.fromMap(map3));
                    query = new QueryManager.ThirdQuery();
                    query.output(writer, query.getFuture(job3).get());
                    logger.info("Finished writing output");
                    break;

                case 4:
                    query = new QueryManager.FourthQuery();
                    break;

                case 5:
                    query = new QueryManager.FifthQuery();
                    break;

                case 6:
                    query = new QueryManager.SixthQuery(arguments.getAmount());
                    break;

                case 7:
                    logger.info("Creating local map with data");
                    final IMap<Long,Pair<String,String>> map7 = client.getMap(mapName);
                    map7.clear();
                    final Map<Long,Pair<String,String>> query7Map = new HashMap<>();
                    timer.start();
                    CSVReader.getDepartmentsAndProvinces(arguments.getInputPath()).stream()
                        .forEach(p -> query7Map.put(count.getAndIncrement(),p));
                    logger.info("Reading data took: " + timer);
                    timer.stop().reset();
                    timer.start();
                    logger.info("Start loading remote data");
                    map7.putAll(query7Map);
                    logger.info("Finished loading remote data");
                    query = new QueryManager.SeventhQuery(arguments.getAmount());
                    Job <Long,Pair<String,String>> job7 = jobTracker.newJob(KeyValueSource.fromMap(map7));
                    query.output(writer, query.getFuture(job7).get());
                    logger.info("The query took: " + timer);
                    timer.stop().reset();
                    logger.info("Finished writing output");
                    break;
            }
//            query.output(writer, query.getFuture(job7).get());

        }catch (IOException e) {
            System.err.println("There was an error writing the output " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

}


