package ar.edu.itba.pod.utilsTest;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import ar.edu.itba.pod.utils.CSVReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CSVReaderTest {

    @Test
    public void CSVReaderHappyTest() {
        final String path = "./src/main/resources/census100.csv";
        Person p = new Person(ActivityCondition.values()[1],12170685,"Río Grande","Tierra del Fuego");
        Collection<Person> people = CSVReader.getPeople(path);
        Assert.assertEquals(100, people.size());
        Assert.assertTrue(people.contains(p));

    }


}
