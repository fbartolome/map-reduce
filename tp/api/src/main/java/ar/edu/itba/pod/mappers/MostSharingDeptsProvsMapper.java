package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import java.util.LinkedList;
import java.util.List;

public class MostSharingDeptsProvsMapper implements Mapper<Long,Pair<String,String>,String,String> {

  private final List<String> provinces = new LinkedList<>();

  public MostSharingDeptsProvsMapper() {
    provinces.add("buenos aires");
    provinces.add("ciudad autónoma de buenos aires");
    provinces.add("santa fe");
    provinces.add("entre ríos");
    provinces.add("córdoba");
    provinces.add("san luis");
    provinces.add("mendoza");
    provinces.add("san juan");
    provinces.add("la rioja");
    provinces.add("la pampa");
    provinces.add("río negro");
    provinces.add("chubut");
    provinces.add("santa cruz");
    provinces.add("tierra del fuego");
    provinces.add("corrientes");
    provinces.add("misiones");
    provinces.add("formosa");
    provinces.add("chaco");
    provinces.add("tucumán");
    provinces.add("jujuy");
    provinces.add("salta");
    provinces.add("santiago del estero");
    provinces.add("catamarca");
  }

  @Override
  public void map(Long aLong, Pair<String,String> pair, Context<String, String> context) {
    for (String p : provinces){
      final String department = pair.getKey();
      final String province = pair.getValue().toLowerCase();
      if(p.compareTo(province) != 0){
        String twoProvinces = p.compareTo(province) < 0 ?
            p + " + " + province : province + " + " + p;
        context.emit(twoProvinces,department);
      }
    }
  }
}
