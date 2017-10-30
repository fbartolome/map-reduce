package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class AvgInhabitantsPerHouseholdByRegionReducerFactory implements ReducerFactory<String, Integer, Double> {

    @Override
    public Reducer<Integer, Double> newReducer(String s) {
        return new AvgInhabitantsPerHouseholdByRegion();
    }

    private class AvgInhabitantsPerHouseholdByRegion extends Reducer<Integer, Double>{

        //TODO ver si seguir usando mapa porque al pedo
        //Key is house id and value is the amount of inhabitants in said house
        private ConcurrentHashMap<Integer, Integer> amountOfInhabitantsPerHousehold = new ConcurrentHashMap<>();
        private Double population = new Double(0);

        @Override
        public void reduce(Integer houseId) {
            if(amountOfInhabitantsPerHousehold.contains(houseId)){
                Integer amount = amountOfInhabitantsPerHousehold.get(houseId);
                amountOfInhabitantsPerHousehold.put(houseId, amount + 1);
            }else{
                amountOfInhabitantsPerHousehold.put(houseId, 1);
            }
            population++;
        }

        @Override
        public Double finalizeReduce() {
            return population / amountOfInhabitantsPerHousehold.keySet().size();
        }
    }
}
