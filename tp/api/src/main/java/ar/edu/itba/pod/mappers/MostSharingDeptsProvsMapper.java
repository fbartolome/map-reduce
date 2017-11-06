package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.Pair;
import ar.edu.itba.pod.model.ProvinceMapper;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import java.util.LinkedList;
import java.util.List;

public class MostSharingDeptsProvsMapper implements Mapper<Long,Pair<String,Character>,Pair<Character,Character>,String> {

  @Override
  public void map(Long aLong, Pair<String, Character> pair, Context<Pair<Character, Character>, String> context) {
    for(char i=0; i<ProvinceMapper.maxProvinces; i++){
      final String department = pair.getKey();
      final char province = pair.getValue();
      if(province != i){
        Pair<Character, Character> twoProvinces = i<province ?
            new Pair<>(i,province): new Pair<>(province, i);
        context.emit(twoProvinces,department);
      }
    }
  }
}
