package ar.edu.itba.pod.utils;

public class ConsoleArguments {

    private String[] ips;
    private int queryNumber;
    private String inputPath;
    private String outPath;
    private String timeOutPath;
    private Integer amount;
    private String province;

    public ConsoleArguments(String[] ips, int queryNumber, String inputPath, String outPath, String timeOutPath) {
        this.ips = ips;
        this.queryNumber = queryNumber;
        this.inputPath = inputPath;
        this.outPath = outPath;
        this.timeOutPath = timeOutPath;
    }

    public void setAmount(Integer amount) {
        if(queryNumber!=2 && queryNumber!=6 && queryNumber!=7) throw new IllegalStateException("There is no amount in query " + queryNumber);
        this.amount = amount;
    }

    public void setProvince(String province) {
        if(queryNumber!=2) throw new IllegalStateException("There is no province in query " + queryNumber);
        this.province = province;
    }

    public String[] getIps() {
        return ips;
    }

    public int getQueryNumber() {
        return queryNumber;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutPath() {
        return outPath;
    }

    public String getTimeOutPath() {
        return timeOutPath;
    }

    public Integer getAmount() {
        if(queryNumber!=2 && queryNumber!=6 && queryNumber!=7) throw new IllegalStateException("There is no amount in query " + queryNumber);

        return amount;
    }

    public String getProvince() {
        if(queryNumber!=2) throw new IllegalStateException("There is no province in query " + queryNumber);
        return province;
    }
}
