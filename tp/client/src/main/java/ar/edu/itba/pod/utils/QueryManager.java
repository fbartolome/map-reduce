package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.collators.MinAmountAndOrderCollator;
import ar.edu.itba.pod.collators.OrderByCollator;
import ar.edu.itba.pod.collators.TopAndOrderByCollator;
import ar.edu.itba.pod.combiners.AddCombinerFactory;
import ar.edu.itba.pod.combiners.UnemploymentByRegionCombinerFactory;
import ar.edu.itba.pod.mappers.KeyValueMapper;
import ar.edu.itba.pod.mappers.MostSharingDeptsProvsMapper;
import ar.edu.itba.pod.mappers.UnitMapper;
import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Pair;
import ar.edu.itba.pod.model.ProvinceMapper;
import ar.edu.itba.pod.model.RegionMapper;
import ar.edu.itba.pod.reducers.CountReducerFactory;
import ar.edu.itba.pod.reducers.InhabitantsPerHouseholdByRegionReducerFactory;
import ar.edu.itba.pod.reducers.MostSharingDeptsProvsReducerFactory;
import ar.edu.itba.pod.reducers.Query3ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

public class QueryManager {

    private static Logger logger = LoggerFactory.getLogger(QueryManager.class);

    private static <T,U> String defaultFormatter(Entry<T,U> e) {
        return e.getKey() + "," + e.getValue() + "\n";
    }

    private static <T,U> String regionFormatter(Entry<T,U> e) {
        return RegionMapper.getRegion((Character)e.getKey()) + "," + e.getValue() + "\n";
    }

    private static <T,U> String provinceSumFormatter(Entry<Pair<Character, Character>,U> e) {
        return
                ProvinceMapper.getProvince(e.getKey().getKey()) + " + " +
                        ProvinceMapper.getProvince(e.getKey().getValue()) +
                        "," + e.getValue() + "\n";
    }

    private static <T,U> void output(PrintWriter writer, List<Entry<T,U>> response, Function<Entry<T,U>, String> formatter) {
        for (Entry<T, U> entry : response) {
            writer.write(formatter.apply(entry));
        }
        writer.flush();
    }

    /**
     * Key in: id
     * Value in: region
     * Key out: region
     * Value in: people in region
     */
    static public class FirstQuery implements Query<Long,Character,List<Entry<Character,Long>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Long>>> getFuture(Job<Long, Character> job) {
            return job
                    .mapper(new UnitMapper())
                    .combiner(new AddCombinerFactory(0))
                    .reducer(new CountReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Long>> response) {
            QueryManager.output(writer, response, QueryManager::regionFormatter);
        }
    }

    /**
     * Key in: id
     * Value in: department
     * Key out: department
     * Value in: people in department
     */
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
                    .combiner(new AddCombinerFactory(0))
                    .reducer(new CountReducerFactory())
                    .submit(new TopAndOrderByCollator(n, false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<String, Long>> response) {
            QueryManager.output(writer, response, QueryManager::defaultFormatter);
        }
    }

    /**
     * Key in: id
     * Value in: pair of region and activity condition
     * Key out: region
     * Value in: unemployment index
     */
    static public class ThirdQuery implements Query<Long, Pair<Character, ActivityCondition>,List<Entry<Character,Double>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Double>>> getFuture(Job<Long, Pair<Character, ActivityCondition>> job) {
            return job
                    .mapper(new KeyValueMapper())
                    .combiner(new UnemploymentByRegionCombinerFactory())
                    .reducer(new Query3ReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Double>> response) {
            QueryManager.output(writer, response, QueryManager::regionFormatter);
        }
    }

    /**
     * Key in: household id
     * Value in: region
     * Key out: region
     * Value in: households in region
     */
    static public class FourthQuery implements Query<Integer,Character,List<Entry<Character,Long>>> {

        @Override
        public ICompletableFuture<List<Entry<Character, Long>>> getFuture(Job<Integer, Character> job) {
            return job
                    .mapper(new UnitMapper())
                    .combiner(new AddCombinerFactory(0))
                    .reducer(new CountReducerFactory())
                    .submit(new OrderByCollator(false, false));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Character, Long>> response) {
            QueryManager.output(writer, response, QueryManager::regionFormatter);
        }
    }

    /**
     * Key in: id
     * Value in: pair of region and household id
     * Key out: region
     * Value in: people per household
     */
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
            QueryManager.output(writer, response, QueryManager::regionFormatter);
        }
    }

    /**
     * Key in: id
     * Value in: pair of department and province
     * Key out: department
     * Value in: amount of provinces
     */
    static public class SixthQuery implements Query<Long,String,List<Entry<String,Integer>>> {
        private int n;

        public SixthQuery(int n) {
            this.n = n;
        }

        @Override
        public ICompletableFuture<List<Entry<String, Integer>>> getFuture(Job<Long, String> job) {
            return job
                    .mapper(new UnitMapper())
                    .combiner(new AddCombinerFactory<>(0))
                    .reducer(new CountReducerFactory())
                    .submit(new MinAmountAndOrderCollator(false, false, n));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<String, Integer>> response) {
            QueryManager.output(writer, response, QueryManager::defaultFormatter);
        }
    }


    /**
     * Key in: id
     * Value in: pair of department and province
     * Key out: Pair of province 1 and province 2
     * Value in: amount of departments
     */
    static public class SeventhQuery implements Query<Long,Pair<String,Character>,List<Entry<Pair<Character,Character>,Integer>>> {
        private Integer n;

        public SeventhQuery(Integer n) {
            this.n = n;
        }

        @Override
        public ICompletableFuture<List<Entry<Pair<Character,Character>, Integer>>> getFuture(Job<Long,Pair<String,Character>> job) {
            return job
                .mapper(new MostSharingDeptsProvsMapper())
                .reducer(new MostSharingDeptsProvsReducerFactory())
                .submit(new MinAmountAndOrderCollator(false,false,n));
        }

        @Override
        public void output(PrintWriter writer, List<Entry<Pair<Character,Character>, Integer>> response) {
            QueryManager.output(writer, response, QueryManager::provinceSumFormatter);
        }
    }

}
