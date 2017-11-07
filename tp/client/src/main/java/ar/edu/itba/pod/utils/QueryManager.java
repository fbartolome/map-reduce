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

import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

public class QueryManager {


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
     *
     * Total population per region, ordered decreasingly by the total population
     *
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
     *
     *the n most populated departments in a particular province (the province has already been filtered previously)
     *
     * Key in: id
     * Value in: department
     * Key out: department
     * Value in: people in department
     */
    static public class SecondQuery implements Query<Long,String,List<Entry<String,Long>>> {

        private int n;

        public SecondQuery(int n) {
            this.n = n;
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
     *
     * Unemployement index in each region of the country, ordered decreasingly by index
     *
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
     *
     * Total amount of homes in each region ordered decreasingly by the total amount of homes
     *
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
     *
     * Average amount of people per house in each region, ordered decreasingly by avg
     *
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
     * departments that appear in at least n distinct provinces, ordered decreasingly by number of apparitions
     *
     * Key in: id
     * Value in: department
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
     *
     * Pair of provinces that share at least n department names
     *
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
