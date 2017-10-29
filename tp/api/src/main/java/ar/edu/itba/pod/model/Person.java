package ar.edu.itba.pod.model;

import java.io.Serializable;

public class Person implements Serializable{

    final private ActivityCondition activityCondition;
    final private int homeId;
    final private String departmentName;
    final private String provinceName;
    final private String region;

    public Person(ActivityCondition activityCondition, int homeId,
        String departmentName, String provinceName) {
        this.activityCondition = activityCondition;
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.provinceName = provinceName;
        this.region = Regions.getRegion(provinceName);
    }

    public ActivityCondition getActivityCondition() {
        return activityCondition;
    }

    public int getHomeId() {
        return homeId;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public String getRegion() {
        return region;
    }
}
