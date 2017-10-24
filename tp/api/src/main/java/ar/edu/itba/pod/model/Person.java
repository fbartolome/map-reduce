package ar.edu.itba.pod.model;

import java.io.Serializable;

public class Person implements Serializable{

    private ActivityCondition activityCondition;

    private int homeId;

    private String departmentName;

    private String provinceName;

    public Person(ActivityCondition activityCondition, int homeId, String departmentName, String provinceName) {
        this.activityCondition = activityCondition;
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.provinceName = provinceName;
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
}
