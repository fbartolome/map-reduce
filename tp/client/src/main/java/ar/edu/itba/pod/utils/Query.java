package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.io.PrintWriter;

public interface Query<KeyIn,ValueIn,ValueOut> {

    ICompletableFuture<ValueOut> getFuture(Job<KeyIn,ValueIn> job);

    void output(PrintWriter writer, ValueOut response);
}
