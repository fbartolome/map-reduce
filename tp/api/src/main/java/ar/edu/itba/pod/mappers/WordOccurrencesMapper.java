package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class WordOccurrencesMapper implements Mapper<String,String,String,Long>{

    final String book;

    public WordOccurrencesMapper(String book) {
        this.book = book;
    }

    @Override
    public void map(String s, String s2, Context<String, Long> context) {
        if(!s.equals(book)){
            return;
        }
//        System.out.println("AHHHHHHHHHHH");

        String[] words = s2.split(" ");
        for(String word : words){
            context.emit(word,new Long(1));
        }
    }
}
