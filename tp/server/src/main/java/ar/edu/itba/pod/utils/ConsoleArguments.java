package ar.edu.itba.pod.utils;

public class ConsoleArguments {

    private String[] ips;
    private int amount;
    private String province;

    public ConsoleArguments(String[] ips) {
        this.ips = ips;
    }


    public String[] getIps() {
        return ips;
    }

}
