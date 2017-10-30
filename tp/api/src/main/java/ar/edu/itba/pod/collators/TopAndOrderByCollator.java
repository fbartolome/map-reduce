package ar.edu.itba.pod.collators;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TopAndOrderByCollator<K extends Comparable<K>,V extends Comparable<V>>
        extends OrderByCollator<K,V> {

    private final int top;

    public TopAndOrderByCollator(int top, boolean ascending, boolean byKey) {
        super(ascending, byKey);
        this.top = top;
    }

    @Override
    public List<Map.Entry<K,V>> collate(Iterable<Map.Entry<K, V>> iterable) {
        List<Map.Entry<K,V>> ret = super.collate(iterable);
        return ret.subList(0,top > ret.size() ? ret.size() : top);
    }
}
