package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.collators.MinAmountAndOrderCollator;
import ar.edu.itba.pod.collators.OrderByCollator;
import ar.edu.itba.pod.collators.TopAndOrderByCollator;
import ar.edu.itba.pod.mappers.*;
import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Person;
import ar.edu.itba.pod.model.RegionMapper;
import ar.edu.itba.pod.reducers.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.synth.Region;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

public class QueryManager {

    private static Logger logger = LoggerFactory.getLogger(QueryManager.class);

    private static <T,U> String defaultFormatter(Entry<T,U> e) {
        return e.getKey() + "," + e.getValue() + "\n";
    }

    private static <T,U> String regionFommater(Entry<T,U> e) {
        return RegionMapper.getRegion((Character)e.getKey()) + "," + e.getValue() + "\n";
    }

    private static <T,U> void output(PrintWriter writer, List<Entry<T,U>> response, Function<Entry<T,U>, String> formatter) {
        for (Entry<T, U> entry : response) {
            writer.write(formatter.apply(entry));
        }
        writer.flush();
    }

    static public class FirstQuery implements Query<Long,Character,List<Entry<Character,Long>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Long>>> getFuture(Job<Long, Character> job) {
            logger.debug("getFuture in first Query");
            return job
                    .mapper(new UnitMapper())
                    .reducer(new CountReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Long>> response) {
            logger.debug("output first query");
            QueryManager.output(writer, response, QueryManager::regionFommater);
        }
    }

    static public class SecondQuery implements Query<Long,String,List<Entry<String,Long>>> {

        private int n;
        private String prov;

        public SecondQuery(int n, String prov) {
            this.n = n;
            this.prov = prov;
        }

        @Override
        public ICompletableFuture<List<Entry<String, Long>>> getFuture(Job<Long, String> job) {
            return job
                    .mapper(new UnitMapper())
                    .reducer(new CountReducerFactory())
                    .submit(new TopAndOrderByCollator(n, false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<String, Long>> response) {
            QueryManager.output(writer, response, QueryManager::defaultFormatter);
        }
    }

    static public class ThirdQuery implements Query<Long, Pair<Character, ActivityCondition>,List<Entry<Character,Double>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Double>>> getFuture(Job<Long, Pair<Character, ActivityCondition>> job) {
            return job
                    .mapper(new KeyValueMapper())
                    .reducer(new UnemploymentByRegionReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Double>> response) {
            QueryManager.output(writer, response, QueryManager::regionFommater);
        }
    }

    static public class FourthQuery implements Query<Integer,Character,List<Entry<Character,Long>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Long>>> getFuture(Job<Integer, Character> job) {
            return job
                    .mapper(new UnitMapper())
                    .reducer(new CountReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Long>> response) {
            QueryManager.output(writer, response, QueryManager::regionFommater);
        }
    }

    static public class FifthQuery implements Query<Long,Pair<Character,Integer>,List<Entry<Character,Double>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Double>>> getFuture(Job<Long,Pair<Character,Integer>> job) {
            return job
                    .mapper(new KeyValueMapper())
                    .reducer(new InhabitantsPerHouseholdByRegionReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Double>> response) {
            QueryManager.output(writer, response, QueryManager::regionFommater);
        }
    }

    static public class SixthQuery implements Query<Long,Pair<String, Character>,List<Entry<String,Integer>>> {
        private int n;

        public SixthQuery(int n) {
            this.n = n;
        }

        @Override
        public ICompletableFuture<List<Entry<String, Integer>>> getFuture(Job<Long, Pair<String, Character>> job) {
            return job
                    .mapper(new KeyValueMapper())
                    .reducer(new CountReducerFactory())
                    .submit(new MinAmountAndOrderCollator(false, false, n));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<String, Integer>> response) {
            QueryManager.output(writer, response, QueryManager::defaultFormatter);
        }
    }


    static public class SeventhQuery implements Query<Long,Pair<String,Character>,List<Entry<String,Integer>>> {
        private int n;

        public SeventhQuery(int n) {
            this.n = n;
        }

        @Override
        public ICompletableFuture<List<Entry<String, Integer>>> getFuture(Job<Long,Pair<String,Character>> job) {
            return job
                .mapper(new KeyValueMapper())
                .reducer(new MostSharingDeptsProvsReducerFactory())
                .submit(new MinAmountAndOrderCollator(false,false,n));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<String, Integer>> response) {
            QueryManager.output(writer, response, QueryManager::defaultFormatter);
        }
    }

}
