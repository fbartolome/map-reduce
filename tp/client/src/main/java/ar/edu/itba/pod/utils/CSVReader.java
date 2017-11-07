package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

public class CSVReader {

    private static Logger logger = LoggerFactory.getLogger(CSVReader.class);

    public static Map<Long,Character> getRegions(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Long,Character> regions = new HashMap<>();

        try {
            try(BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    lineCounter.increment();
                    regions.put(lineCounter.longValue(),
                            RegionMapper.getKey(Regions.getRegion(lineParts[3])));
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading from CSV ...");
        return regions;
    }

    public static Map<Long, Pair<Character, ActivityCondition>> getConditionByRegion(String path) {
        logger.info("Start reading from CSV ...");
        Map<Long, Pair<Character, ActivityCondition>> query = new HashMap<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    lineCounter.increment();
                    query.put(lineCounter.longValue(),
                            new Pair(
                                    RegionMapper.getKey(Regions.getRegion(lineParts[3])),
                                    new ActivityCondition(Integer.valueOf(lineParts[0]))
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

    public static Map<Integer, Character> getHomesByHomeKey(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Integer, Character> query = new HashMap<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    query.put(Integer.valueOf(lineParts[1]),RegionMapper.getKey(Regions.getRegion(lineParts[3])));
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading from CSV ...");
        return query;
    }

    public static Map<Long, Pair<Character, Integer>> getHomesByRegionRawData(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Map<Long, Pair<Character, Integer>> query = new HashMap<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                LongAdder lineCounter = new LongAdder();
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    lineCounter.increment();
                    query.put(lineCounter.longValue(),
                            new Pair(
                                    RegionMapper.getKey(Regions.getRegion(lineParts[3])),
                                    Integer.valueOf(lineParts[1])
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


    public static Map<Long, Pair<Character, Integer>> getHomesByRegionLocalFilter(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Set<Pair<Character, Integer>> set = new HashSet<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    set.add(
                            new Pair(
                                    RegionMapper.getKey(Regions.getRegion(lineParts[3])),
                                    Integer.valueOf(lineParts[1])
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<Long, Pair<Character, Integer>> query = new HashMap<>();
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
    public static Map<Long, Pair<String, Character>> getDepartmentsAndProvinces(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Set<Pair<String, Character>> set = new HashSet<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    set.add(
                            new Pair(
                                    lineParts[2],
                                    ProvinceMapper.getKey(lineParts[3])
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<Long, Pair<String, Character>> query = new HashMap<>();
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
    public static Map<Long, String> getDepartmentsInProvinces(String path){
        //read file into stream, try-with-resources
        logger.info("Start reading from CSV ...");
        Set<Pair<String, Character>> set = new HashSet<>();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                reader.lines().forEach(line -> {
                    String[] lineParts = line.split(",");
                    set.add(
                            new Pair(
                                    lineParts[2],
                                    ProvinceMapper.getKey(lineParts[3])
                            )
                    );
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<Long, String> query = new HashMap<>();
        LongAdder lineCounter = new LongAdder();
        set.stream().forEach(p -> {
                    query.put(lineCounter.longValue(), p.getKey());
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