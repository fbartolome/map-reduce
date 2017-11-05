package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ar.edu.itba.pod.model.Regions;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class CSVReader {

    private static Logger logger = LoggerFactory.getLogger(CSVReader.class);

    //not tested yet
    public static Map<Long, Person> getPeople(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Long,Person> people = new HashMap<>();
        try {
            try(BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    int index = 0;
                    lineCounter.increment();
                    people.put(lineCounter.longValue(),
                            new Person(
                                    ActivityCondition.values()[Integer.valueOf(lineParts[index++])],
                                    Integer.valueOf(lineParts[index++]),
                                    lineParts[index++],
                                    lineParts[index])
                    );
                });
            }
            logger.info("Finished reading from CSV ...");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return people;
    }

    public static Map<Long,String> getRegions(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Long,String> regions = new HashMap<>();

        try {
            try(BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    lineCounter.increment();
                    regions.put(lineCounter.longValue(),
                            Regions.getRegion(lineParts[3]));
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading from CSV ...");
        return regions;
    }

    public static Map<Long, Pair<ActivityCondition, String>> getConditionByRegion(String path) {
        logger.info("Start reading from CSV ...");
        Map<Long, Pair<ActivityCondition, String>> query = new HashMap<>();

        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    lineCounter.increment();
                    query.put(lineCounter.longValue(),
                            new Pair(
                                    ActivityCondition.values()[Integer.valueOf(lineParts[0])],
                                    Regions.getRegion(lineParts[3])
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading from CSV ...");
        return query;
    }

    public static Map<Long, Pair<Integer, String>> getHomesByRegionRawData(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Long, Pair<Integer, String>> query = new HashMap<>();

        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    lineCounter.increment();
                    query.put(lineCounter.longValue(),
                            new Pair(
                                    Integer.valueOf(lineParts[1]),
                                    Regions.getRegion(lineParts[3])
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading from CSV ...");
        return query;
    }

    public static Map<Long, Pair<Integer, String>> getHomesByRegionLocalFilter(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Set<Pair<Integer, String>> set = new HashSet<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    set.add(
                            new Pair(
                                    Integer.valueOf(lineParts[1]),
                                    Regions.getRegion(lineParts[3])
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<Long, Pair<Integer, String>> query = new HashMap<>();
        LongAdder lineCounter = new LongAdder();
        set.stream().forEach(p -> {
                query.put(lineCounter.longValue(), p);
                lineCounter.increment();
            }
        );

        logger.info("Finished reading from CSV ...");
        return query;
    }

    //First value of Pair is the department and the second one is the province.
    public static Map<Long, Pair<String, String>> getDepartmentsAndProvinces(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Set<Pair<String, String>> set = new HashSet<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    set.add(
                            new Pair(
                                    lineParts[2],
                                    lineParts[3]
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<Long, Pair<String, String>> query = new HashMap<>();
        LongAdder lineCounter = new LongAdder();
        set.stream().forEach(p -> {
                    query.put(lineCounter.longValue(), p);
                    lineCounter.increment();
                }
        );

        logger.info("Finished reading from CSV ...");
        return query;
    }
    public static Map<Long, String> departmentInProv(String path, String prov){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Long, String> query = new HashMap<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    String department = lineParts[2];
                    String province = lineParts[3];
                    if(prov.equals(province)){
                        lineCounter.increment();
                        query.put(lineCounter.longValue(),
                                department
                        );
                    }
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading from CSV ...");
        return query;
    }

}