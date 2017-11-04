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
        this.provinceName = provinceName.toLowerCase();
        this.region = Regions.getRegion(this.provinceName);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Person person = (Person) o;

        if (homeId != person.homeId) return false;
        if (activityCondition != person.activityCondition) return false;
        if (!departmentName.equals(person.departmentName)) return false;
        return provinceName.equals(person.provinceName);
    }

    @Override
    public int hashCode() {
        return homeId;
    }
}
