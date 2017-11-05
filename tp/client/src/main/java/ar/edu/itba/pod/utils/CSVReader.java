package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class CSVReader {

    private static Logger logger = LoggerFactory.getLogger(CSVReader.class);

    //not tested yet
    public static Collection<Person> readCSV (String path){

        logger.info("Start reading from CSV ...");

        //read file into stream, try-with-resources
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

}