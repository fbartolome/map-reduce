package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Pair;
import ar.edu.itba.pod.utils.*;
import com.google.common.base.Stopwatch;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {

        logger.info("hazelcast Client Starting ...");

        CLIParser cli = new CLIParser(args);
        ConsoleArguments arguments = cli.parse();

        final ClientConfig ccfg = new ClientConfig()
                .setGroupConfig(new GroupConfig()
                        .setName("56382-54308-55291-53559")
                        .setPassword("56382-54308-55291-53559"));
        ccfg.getNetworkConfig()
                .setAddresses(Arrays.asList(arguments.getIps()));
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        JobTracker jobTracker = client.getJobTracker("tracker");

        Stopwatch timer = Stopwatch.createUnstarted();
        try {
            FileWriter fw = new FileWriter(arguments.getTimeOutPath(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter timerFile = new PrintWriter(bw);

            PrintWriter writer = new PrintWriter(arguments.getOutPath(), "UTF-8");
            Query query = null;
            switch (arguments.getQueryNumber()) {

                case 1:
                    logger.info("Creating local map with data");
                    final IMap<Long,Character> map1 = client.getMap("56382-54308-55291-53559-map1");
                    map1.clear();
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    Map<Long,Character> query1Map = CSVReader.getRegions(arguments.getInputPath());
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.info("Start loading remote data");
                    map1.putAll(query1Map);
                    logger.info("Finished loading remote data");
                    query = new QueryManager.FirstQuery();
                    Job <Long,Character> job1 = jobTracker.newJob(KeyValueSource.fromMap(map1));
                    query.output(writer, query.getFuture(job1).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;

                case 2:
                    logger.info("Creating local map with data");
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    Map<Long, String> query2Map = CSVReader.departmentInProv(arguments.getInputPath(), arguments.getProvince());
                    IMap<Long,String> map2 = client.getMap("56382-54308-55291-53559-map2");
                    map2.clear();
                    logger.info("Start loading remote data");
                    map2.putAll(query2Map);
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.debug("map size " + map2.size());
                    logger.info("Finished loading remote data");
                    Job<Long, String> job2 = jobTracker.newJob(KeyValueSource.fromMap(map2));
                    query = new QueryManager.SecondQuery(arguments.getAmount(), arguments.getProvince());
                    query.output(writer, query.getFuture(job2).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;

                case 3:
                    logger.info("Creating local map with data");
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    Map<Long, Pair<Character, ActivityCondition>> auxMap = CSVReader.getConditionByRegion(arguments.getInputPath());
                    IMap<Long, Pair<Character, ActivityCondition>> map3 = client.getMap("56382-54308-55291-53559-map3");
                    map3.clear();
                    logger.info("Start loading remote data");
                    map3.putAll(auxMap);
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.info("Finished loading remote data");
                    Job<Long, Pair<Character, ActivityCondition>> job3 = jobTracker.newJob(KeyValueSource.fromMap(map3));
                    query = new QueryManager.ThirdQuery();
                    query.output(writer, query.getFuture(job3).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;

                case 4:
                    logger.info("Creating local map with data");
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    final IMap<Integer,Character> map4 = client.getMap("56382-54308-55291-53559-map4");
                    map4.clear();
                    Map<Integer,Character> otherMap4 = CSVReader.getHomesByHomeKey(arguments.getInputPath());
                    logger.info("Start loading remote data");
                    map4.putAll(otherMap4);
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.info("Finished loading remote data");
                    query = new QueryManager.FourthQuery();
                    Job <Integer,Character> job4 = jobTracker.newJob(KeyValueSource.fromMap(map4));
                    query.output(writer, query.getFuture(job4).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;

                case 5:
                    logger.info("Creating local map with data");
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    final IMap<Long,Pair<Character,Integer>> map5 = client.getMap("56382-54308-55291-53559-map5");
                    map5.clear();
                    Map<Long,Pair<Character,Integer>> otherMap5 = CSVReader.getHomesByRegionRawData(arguments.getInputPath());
                    logger.info("Start loading remote data");
                    map5.putAll(otherMap5);
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.info("Finished loading remote data");
                    query = new QueryManager.FifthQuery();
                    Job <Long,Pair<Character,Integer>> job5 = jobTracker.newJob(KeyValueSource.fromMap(map5));
                    query.output(writer, query.getFuture(job5).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;

                case 6:
                    logger.info("Creating local map with data");
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    Map<Long, Pair<String, Character>> auxMap6 = CSVReader.getDepartmentsAndProvinces(arguments.getInputPath());
                    IMap<Long, Pair<String, Character>> map6 = client.getMap("56382-54308-55291-53559-map6");
                    map6.clear();
                    logger.info("Start loading remote data");
                    map6.putAll(auxMap6);
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.info("Finished loading remote data");
                    Job<Long, Pair<String, Character>> job6 = jobTracker.newJob(KeyValueSource.fromMap(map6));
                    query = new QueryManager.SixthQuery(arguments.getAmount());
                    query.output(writer, query.getFuture(job6).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;

                case 7:
                    logger.info("Creating local map with data");
                    final IMap<Long,Pair<String,Character>> map7 = client.getMap("56382-54308-55291-53559-map7");
                    map7.clear();
                    timer.start();
                    printTime(timerFile, "Inicio de la lectura del archivo");
                    final Map<Long,Pair<String,Character>> query7Map = CSVReader.getDepartmentsAndProvinces(arguments.getInputPath());
                    logger.info("Start loading remote data");
                    map7.putAll(query7Map);
                    printTime(timerFile, "Fin de la lectura del archivo");
                    timerFile.append("Tiempo de lectura: " + timer).println();
                    timer.stop().reset();
                    timer.start();
                    printTime(timerFile, "Inicio del trabajo map/reduce");
                    logger.info("Finished loading remote data");
                    query = new QueryManager.SeventhQuery(arguments.getAmount());
                    Job <Long,Pair<String,Character>> job7 = jobTracker.newJob(KeyValueSource.fromMap(map7));
                    query.output(writer, query.getFuture(job7).get());
                    printTime(timerFile, "Fin del trabajo map/reduce");
                    timerFile.append("Query" + arguments.getQueryNumber() + " tardó: " + timer).println();
                    timer.stop();
                    logger.info("Finished writing output");
                    break;
            }
            timerFile.close();

        }catch (IOException e) {
            System.err.println("There was an error writing the output " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

    private static void printTime(PrintWriter timerFile, String message){
        timerFile.append(LocalDateTime.now().toString() + " INFO ["
            + Thread.currentThread().getStackTrace()[2].getMethodName()
            + "] " + Thread.currentThread().getStackTrace()[2].getFileName()
            + ":" + Thread.currentThread().getStackTrace()[2].getLineNumber()
            + " - " + message).println();
    }

}


