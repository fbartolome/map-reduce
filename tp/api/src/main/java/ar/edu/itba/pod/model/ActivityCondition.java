package ar.edu.itba.pod.model;

import java.io.Serializable;

public class ActivityCondition implements Serializable {
    public static ActivityCondition NO_DATA = new ActivityCondition((char)0);
    public static ActivityCondition EMPLOYED = new ActivityCondition((char)1);
    public static ActivityCondition UNEMPLOYED = new ActivityCondition((char)2);
    public static ActivityCondition ECONOMICALLY_INACTIVE = new ActivityCondition((char)3);

    private char value;
    public ActivityCondition(int n){
        value = (char)n;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActivityCondition that = (ActivityCondition) o;

        return value == that.value;
    }

    @Override
    public String toString() {
        switch (value) {
            case 0:
               return "NO_DATA";
            case 1:
                return "EMPLOYED";
            case 2:
                return "UNEMPLOYED";
            case 3:
                return "ECONOMICALLY_INACTIVE";
        }
        return "UNDEFINED";
    }

    @Override
    public int hashCode() {
        return (int) value;
    }
}
