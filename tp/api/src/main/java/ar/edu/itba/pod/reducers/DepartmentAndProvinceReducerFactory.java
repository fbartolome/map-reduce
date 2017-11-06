package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class DepartmentAndProvinceReducerFactory implements ReducerFactory<String, String, Integer>{


        @Override
        public Reducer<String, Integer> newReducer(String department) {
            return new DepartmentAndProvinceReducerFactory.DepartmentAndRegionReducer();
        }

        private class DepartmentAndRegionReducer extends Reducer<String, Integer>{

            Set<String> provinces = new HashSet<>();

            @Override
            public void reduce(String province) {
                provinces.add(province);
            }

            @Override
            public Integer finalizeReduce() {
                return provinces.size();
            }
        }
    }
