package ar.edu.itba.pod.model;

import java.io.IOException;
import java.io.Serializable;

public class Pair<K,V> implements Serializable {

  private final K key;
  private final V value;

  public Pair(final K key, final V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }
}
