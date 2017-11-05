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
        logger.info("Start reading from CSV ...");
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
        logger.info("Finished reading from CSV ...");
        return regions;
    }

    public static Collection<Pair<String, ActivityCondition>> getConditionByRegion(String path){
        //read file into stream, try-with-resources
        List<Pair<String, ActivityCondition>> query = new LinkedList<>();
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
            query.add(new Pair<>(region, ac));
        }
        return query;
    }

    public static Collection<Pair<String, Integer>> getHomesByRegionRawData(String path){
        //read file into stream, try-with-resources
        List<Pair<String, Integer>> query = new LinkedList<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return getHomesByRegion(filescanner, query);
    }

    public static Collection<Pair<String, Integer>> getHomesByRegionLocalFilter(String path){
        //read file into stream, try-with-resources
        Set<Pair<String, Integer>> query = new HashSet<>();
        Scanner filescanner = null;
        try {
            filescanner = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return getHomesByRegion(filescanner, query);
    }

    private static Collection<Pair<String, Integer>> getHomesByRegion(Scanner filescanner, Collection<Pair<String, Integer>> collection){
        String line;
        while(filescanner.hasNext()){
            line = filescanner.nextLine();
            Scanner lineScanner = new Scanner(line).useDelimiter(",");
            lineScanner.next();
            Integer homeId = Integer.valueOf(lineScanner.next());
            lineScanner.next();
            String region = Regions.getRegion(lineScanner.next());
            collection.add(new Pair<>(region, homeId));
        }
        return collection;
    }


    //First value of Pair is the department and the second one is the province.
    public static Collection<Pair<String, String>> getDepartmentsAndProvinces(String path){
        //read file into stream, try-with-resources
        Set<Pair<String, String>> query = new HashSet<>();
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
    public static Collection<String> departmentInProv(String path, String prov){

        logger.info("Start reading departments in " + prov + " from CSV ...");

        //read file into stream, try-with-resources
        List<String> departments = new LinkedList<>();
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
            String department = lineScanner.next();
            String province = lineScanner.next();
            if(prov.equals(province)){
                departments.add(department);
            }
        }

        logger.info("Finished reading from CSV ...");
        return departments;
    }

}