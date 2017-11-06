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

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Pair<?, ?> pair = (Pair<?, ?>) o;

    if (!key.equals(pair.key)) {
      return false;
    }
    return value.equals(pair.value);
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
