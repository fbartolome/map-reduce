package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ar.edu.itba.pod.model.Regions;
import javafx.util.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class CSVReader {

    private static Logger logger = LoggerFactory.getLogger(CSVReader.class);

    //not tested yet
    public static Collection<Person> getPeople(String path){
        //read file into stream, try-with-resources
            logger.info("Start reading from CSV ...");
        List<Person> people = new LinkedList<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        while(filescanner.hasNext()){
            line = filescanner.nextLine();
            Scanner lineScanner = new Scanner(line).useDelimiter(",");
            people.add(new Person(ActivityCondition.values()[Integer.valueOf(lineScanner.next())],
                    Integer.valueOf(lineScanner.next()), lineScanner.next(), lineScanner.next()));
        }

        logger.info("Finished reading from CSV ...");
        return people;
    }

    public static Collection<String> getRegions(String path){
        //read file into stream, try-with-resources
        List<String> regions = new LinkedList<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        while(filescanner.hasNext()){
            line = filescanner.nextLine();
            Scanner lineScanner = new Scanner(line).useDelimiter(",");
            lineScanner.next();
            lineScanner.next();
            lineScanner.next();
            regions.add(Regions.getRegion(lineScanner.next()));
        }
        return regions;
    }

    public static Collection<Pair<ActivityCondition, String>> getConditionByRegion(String path){
        //read file into stream, try-with-resources
        List<Pair<ActivityCondition, String>> query = new LinkedList<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        while(filescanner.hasNext()){
            line = filescanner.nextLine();
            Scanner lineScanner = new Scanner(line).useDelimiter(",");
            ActivityCondition ac = ActivityCondition.values()[Integer.valueOf(lineScanner.next())];
            lineScanner.next();
            lineScanner.next();
            String region = Regions.getRegion(lineScanner.next());
            query.add(new Pair<>(ac, region));
        }
        return query;
    }

    public static Collection<Pair<Integer, String>> getHomesByRegionRawData(String path){
        //read file into stream, try-with-resources
        List<Pair<Integer, String>> query = new LinkedList<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return getHomesByRegion(filescanner, query);
    }

    public static Collection<Pair<Integer, String>> getHomesByRegionLocalFilter(String path){
        //read file into stream, try-with-resources
        Set<Pair<Integer, String>> query = new HashSet<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return getHomesByRegion(filescanner, query);
    }

    private static Collection<Pair<Integer, String>> getHomesByRegion(Scanner filescanner, Collection<Pair<Integer, String>> collection){
        String line;
        while(filescanner.hasNext()){
            line = filescanner.nextLine();
            Scanner lineScanner = new Scanner(line).useDelimiter(",");
            lineScanner.next();
            Integer homeId = Integer.valueOf(lineScanner.next());
            lineScanner.next();
            String region = Regions.getRegion(lineScanner.next());
            collection.add(new Pair<>(homeId, region));
        }
        return collection;
    }


    //First value of Pair is the department and the second one is the province.
    public static Collection<Pair<String, String>> getDepartmentsAndProvinces(String path){
        //read file into stream, try-with-resources
        List<Pair<String, String>> query = new LinkedList<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        while(filescanner.hasNext()){
            line = filescanner.nextLine();
            Scanner lineScanner = new Scanner(line).useDelimiter(",");
            lineScanner.next();
            lineScanner.next();
            query.add(new Pair<>(lineScanner.next(), lineScanner.next()));
        }
        return query;
    }
}