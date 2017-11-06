package ar.edu.itba.pod.collators;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MinAmountAndOrderCollator <K extends Comparable<K>>
        extends OrderByCollator<K,Long> {

    private Integer minAmount;

    public MinAmountAndOrderCollator(boolean ascending, boolean byKey, Integer minAmount) {
        super(ascending, byKey);
        this.minAmount = minAmount;
    }

    @Override
    public List<Map.Entry<K, Long>> collate(Iterable<Map.Entry<K, Long>> iterable) {
        List<Map.Entry<K, Long>> ret = super.collate(iterable);
        return ret.stream().filter(e -> e.getValue() >= minAmount).collect(Collectors.toList());
    }






}
