package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.Person;
import ar.edu.itba.pod.utils.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static String mapName = "people";

    public static void main(String[] args) {

        CLIParser cli = new CLIParser(args);

        ConsoleArguments arguments = cli.parse();

        logger.info("hazelcast Client Starting ...");

        final ClientConfig ccfg = new ClientConfig();

        ccfg.getNetworkConfig().setAddresses(Arrays.asList(arguments.getIps()));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        IMap<Long,Person> map = client.getMap(mapName);
        final AtomicLong count = new AtomicLong(0);

        //"./client/src/main/resources/census100.csv"
        CSVReader.readCSV(arguments.getInputPath()).stream().forEach(p -> map.put(count.getAndIncrement(), p));




        JobTracker jobTracker = client.getJobTracker("tracker");
        Job<Long,Person> job = jobTracker.newJob(KeyValueSource.fromMap(map));

        try {
            PrintWriter writer = new PrintWriter(arguments.getOutPath(), "UTF-8");
            Query query = null;
            switch (arguments.getQueryNumber()) {

                case 1:
                    query = new QueryManager.FirstQuery();
                    break;

                case 2:
                    query = new QueryManager.SecondQuery(arguments.getAmount(), arguments.getProvince());
                    break;

                case 3:
                    query = new QueryManager.ThirdQuery();
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
                    //TODO
                    System.out.println("NO ESTA IMPLEMENTADO");
                    break;
            }

            query.output(writer, query.getFuture(job).get());

        }catch (IOException e) {
            System.err.println("There was an error writing the output " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

}


