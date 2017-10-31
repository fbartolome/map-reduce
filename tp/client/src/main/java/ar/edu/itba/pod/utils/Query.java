package ar.edu.itba.pod.utils;

import ar.edu.itba.pod.model.Person;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.io.PrintWriter;

public interface Query<T> {
    ICompletableFuture<T> getFuture(Job<Long,Person> job);
    void output(PrintWriter writer, T response);
}
