package com.flink.sustainability.NYT;

public class NYTProfitReport {
    public long windowEnd;
    public int cellIdWE;
    public int cellIdNS;
    public double profit;

    public NYTProfitReport() {}

    public NYTProfitReport(long windowEnd, int cellIdWE, int cellIdNS, double profit) {
        this.windowEnd = windowEnd;
        this.cellIdWE = cellIdWE;
        this.cellIdNS = cellIdNS;
        this.profit = profit;
    }

    @Override
    public String toString() {
        return "NYTProfitReport{" +
                "windowEnd=" + windowEnd +
                ", cellIdWE=" + cellIdWE +
                ", cellIdNS=" + cellIdNS +
                ", profit=" + profit +
                '}';
    }
}
