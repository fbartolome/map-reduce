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
        this.region = calculateRegion(provinceName);
    }

    private String calculateRegion(String provinceName) {
        switch (provinceName) {
            case "Buenos Aires":
            case "Ciudad Autónoma de Buenos Aires":
                return "Región Buenos Aires";
            case "Santa Fe":
            case "Entre Ríos":
            case "Córdoba":
                return "Región Centro";
            case "San Luis":
            case "Mendoza":
            case "San Juan":
            case "La Rioja":
                return "Región del Nuevo Cuyo";
            case "La Pampa":
            case "Neuquén":
            case "Río negro":
            case "Chubut":
            case "Santa Cruz":
            case "Tierra del Fuego":
                return "Región Patagónica";
            case "Corrientes":
            case "Misiones":
            case "Formosa":
            case "Chaco":
            case "Tucumán":
            case "Jujuy":
            case "Salta":
            case "Santiago del Estero":
            case "Catamarca":
                return "Región del Norte Grande Argentino";
            default:
                throw new IllegalArgumentException(provinceName + " is not a valid province");
        }
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
