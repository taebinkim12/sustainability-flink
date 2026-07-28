package com.flink.sustainability.NYT;

public class NYTProfitabilityReport {
    public long windowEnd;
    public int cellIdWE;
    public int cellIdNS;
    public double profitability;

    public NYTProfitabilityReport() {}

    public NYTProfitabilityReport(long windowEnd, int cellIdWE, int cellIdNS, double profitability) {
        this.windowEnd = windowEnd;
        this.cellIdWE = cellIdWE;
        this.cellIdNS = cellIdNS;
        this.profitability = profitability;
    }

    @Override
    public String toString() {
        return "NYTProfitabilityReport{" +
                "windowEnd=" + windowEnd +
                ", cellIdWE=" + cellIdWE +
                ", cellIdNS=" + cellIdNS +
                ", profitability=" + profitability +
                '}';
    }
}
