package com.flink.sustainability.NYT;

public class NYTEmptyTaxiCountReport {
    public long windowEnd;
    public int cellIdWE;
    public int cellIdNS;
    public int count;

    public NYTEmptyTaxiCountReport() {}

    public NYTEmptyTaxiCountReport(long windowEnd, int cellIdWE, int cellIdNS, int count) {
        this.windowEnd = windowEnd;
        this.cellIdWE = cellIdWE;
        this.cellIdNS = cellIdNS;
        this.count = count;
    }

    @Override
    public String toString() {
        return "NYTEmptyTaxiCountReport{" +
                "windowEnd=" + windowEnd +
                ", cellIdWE=" + cellIdWE +
                ", cellIdNS=" + cellIdNS +
                ", count=" + count +
                '}';
    }
}
