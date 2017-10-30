package ar.edu.itba.pod.collators;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MinAmountAndOrderCollator <K extends Comparable<K>>
        extends OrderByCollator<K,Integer> {

    private Integer minAmount;

    public MinAmountAndOrderCollator(boolean ascending, boolean byKey, Integer minAmount) {
        super(ascending, byKey);
        this.minAmount = minAmount;
    }

    @Override
    public List<Map.Entry<K,Integer>> collate(Iterable<Map.Entry<K, Integer>> iterable) {
        List<Map.Entry<K,Integer>> ret = super.collate(iterable);
        return ret.stream().filter(e -> minAmount.compareTo(e.getValue()) <= 0).collect(Collectors.toList());
    }






}
