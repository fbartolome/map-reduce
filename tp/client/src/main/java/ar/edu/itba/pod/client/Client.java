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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {

    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {

        logger.info("Hazelcast Client Starting ...");

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

        try (PrintWriter timerFile = new PrintWriter(arguments.getTimeOutPath(), "UTF-8");
             PrintWriter writer = new PrintWriter(arguments.getOutPath(), "UTF-8") ){

            switch (arguments.getQueryNumber()) {

                case 1:
                    //Total population per region, ordered decreasingly by the total population
                    firstQuery(client, timer, timerFile, arguments.getInputPath(), writer);
                    break;

                case 2:
                    //The n most populated departments in a particular province
                    secondQuery(client, timer, timerFile, arguments.getInputPath(), writer, arguments.getProvince(), arguments.getAmount());
                    break;

                case 3:
                    //Unemployement index in each region of the country, ordered decreasingly by index
                    thirdQuery(client, timer, timerFile, arguments.getInputPath(), writer);
                    break;

                case 4:
                    //Total amount of homes in each region ordered decreasingly by the total amount of homes
                    fourthQuery(client, timer, timerFile, arguments.getInputPath(), writer);
                    break;

                case 5:
                    //Average amount of people per house in each region, ordered decreasingly by avg
                    fifthQuery(client, timer, timerFile, arguments.getInputPath(), writer);
                    break;

                case 6:
                    //Departments that appear in at least n distinct provinces, ordered decreasingly by number of apparitions
                    sixthQuery(client, timer, timerFile, arguments.getInputPath(), writer, arguments.getAmount());
                    break;

                case 7:
                    //Pair of provinces that share at least n department names
                    seventhQuery(client, timer, timerFile, arguments.getInputPath(), writer, arguments.getAmount());
                    break;
            }
            timerFile.close();

        }catch (IOException e) {
            System.err.println("There was an error writing the output " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.shutdown();
        }


    }

    private static void printTime(PrintWriter timerFile, String message){
        timerFile.append(LocalDateTime.now().toString() + " INFO ["
                + Thread.currentThread().getStackTrace()[2].getMethodName()
                + "] " + Thread.currentThread().getStackTrace()[2].getFileName()
                + ":" + Thread.currentThread().getStackTrace()[2].getLineNumber()
                + " - " + message).println();
    }

    private static void firstQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        final IMap<Long,Character> map1 = client.getMap("56382-54308-55291-53559-map1");
        map1.clear();
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        Map<Long,Character> query1Map = CSVReader.getRegions(inputPath);
        printTime(timerFile, "Fin de la lectura del archivo");
        timerFile.append("Tiempo de lectura: " + timer).println();
        timer.stop().reset();
        timer.start();
        printTime(timerFile, "Inicio del trabajo map/reduce");
        logger.info("Start loading remote data");
        map1.putAll(query1Map);
        logger.info("Finished loading remote data");
        Query query = new QueryManager.FirstQuery();
        Job <Long,Character> job1 = jobTracker.newJob(KeyValueSource.fromMap(map1));
        query.output(writer, query.getFuture(job1).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 1 tardó: " + timer).println();
        timer.stop();
        logger.info("Finished writing output");

    }

    private static void secondQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer, String province, Integer n) throws ExecutionException, InterruptedException {

        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        Map<Long, String> query2Map = CSVReader.departmentInProv(inputPath, province);
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
        Query query = new QueryManager.SecondQuery(n);
        query.output(writer, query.getFuture(job2).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 2 tardó: " + timer).println();
        timer.stop();
    }


    public static void thirdQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        Map<Long, Pair<Character, ActivityCondition>> auxMap = CSVReader.getConditionByRegion(inputPath);
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
        Query query = new QueryManager.ThirdQuery();
        query.output(writer, query.getFuture(job3).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 3 tardó: " + timer).println();
        timer.stop();
        logger.info("Finished writing output");
    }

    public static void fourthQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        final IMap<Integer,Character> map4 = client.getMap("56382-54308-55291-53559-map4");
        map4.clear();
        Map<Integer,Character> otherMap4 = CSVReader.getHomesByHomeKey(inputPath);
        logger.info("Start loading remote data");
        map4.putAll(otherMap4);
        printTime(timerFile, "Fin de la lectura del archivo");
        timerFile.append("Tiempo de lectura: " + timer).println();
        timer.stop().reset();
        timer.start();
        printTime(timerFile, "Inicio del trabajo map/reduce");
        logger.info("Finished loading remote data");
        Query query = new QueryManager.FourthQuery();
        Job <Integer,Character> job4 = jobTracker.newJob(KeyValueSource.fromMap(map4));
        query.output(writer, query.getFuture(job4).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 4 tardó: " + timer).println();
        timer.stop();
        logger.info("Finished writing output");
    }

    public static void fifthQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        final IMap<Long,Pair<Character,Integer>> map5 = client.getMap("56382-54308-55291-53559-map5");
        map5.clear();
        Map<Long,Pair<Character,Integer>> otherMap5 = CSVReader.getHomesByRegionRawData(inputPath);
        logger.info("Start loading remote data");
        map5.putAll(otherMap5);
        printTime(timerFile, "Fin de la lectura del archivo");
        timerFile.append("Tiempo de lectura: " + timer).println();
        timer.stop().reset();
        timer.start();
        printTime(timerFile, "Inicio del trabajo map/reduce");
        logger.info("Finished loading remote data");
        Query query = new QueryManager.FifthQuery();
        Job <Long,Pair<Character,Integer>> job5 = jobTracker.newJob(KeyValueSource.fromMap(map5));
        query.output(writer, query.getFuture(job5).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 5 tardó: " + timer).println();
        timer.stop();
        logger.info("Finished writing output");
    }

    public static void sixthQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer, Integer n) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        Map<Long, String> auxMap6 = CSVReader.getDepartmentsInProvinces(inputPath);
        IMap<Long, String> map6 = client.getMap("56382-54308-55291-53559-map6");
        map6.clear();
        logger.info("Start loading remote data");
        map6.putAll(auxMap6);
        printTime(timerFile, "Fin de la lectura del archivo");
        timerFile.append("Tiempo de lectura: " + timer).println();
        timer.stop().reset();
        timer.start();
        printTime(timerFile, "Inicio del trabajo map/reduce");
        logger.info("Finished loading remote data");
        Job<Long, String> job6 = jobTracker.newJob(KeyValueSource.fromMap(map6));
        Query query = new QueryManager.SixthQuery(n);
        query.output(writer, query.getFuture(job6).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 6 tardó: " + timer).println();
        timer.stop();
        logger.info("Finished writing output");
    }

    public static void seventhQuery(HazelcastInstance client, Stopwatch timer, PrintWriter timerFile, String inputPath, PrintWriter writer, Integer n) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = client.getJobTracker("tracker");
        logger.info("Creating local map with data");
        final IMap<Long,Pair<String,Character>> map7 = client.getMap("56382-54308-55291-53559-map7");
        map7.clear();
        timer.start();
        printTime(timerFile, "Inicio de la lectura del archivo");
        final Map<Long,Pair<String,Character>> query7Map = CSVReader.getDepartmentsAndProvinces(inputPath);
        logger.info("Start loading remote data");
        map7.putAll(query7Map);
        printTime(timerFile, "Fin de la lectura del archivo");
        timerFile.append("Tiempo de lectura: " + timer).println();
        timer.stop().reset();
        timer.start();
        printTime(timerFile, "Inicio del trabajo map/reduce");
        logger.info("Finished loading remote data");
        Query query = new QueryManager.SeventhQuery(n);
        Job <Long,Pair<String,Character>> job7 = jobTracker.newJob(KeyValueSource.fromMap(map7));
        query.output(writer, query.getFuture(job7).get());
        printTime(timerFile, "Fin del trabajo map/reduce");
        timerFile.append("Query 7 tardó: " + timer).println();
        timer.stop();
        logger.info("Finished writing output");
    }



}


