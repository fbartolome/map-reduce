package ar.edu.itba.pod.model;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class RegionMapper {
    private static Map<String,Character> provinceKey = new HashMap<>();
    private static Map<Character,String> reverseProvinceKey = new HashMap<>();

    static {
        LongAdder adder = new LongAdder();
        Regions.getRegions().forEach((p) -> {
                    provinceKey.put(p, (char)adder.longValue());
                    reverseProvinceKey.put((char)adder.longValue(), p);
                    adder.increment();
                }
        );
    }

    public static char getKey(String region) {
        return provinceKey.get(region);
    }

    public static String getRegion(char key) {
        return reverseProvinceKey.get(key);
    }
}
