package com.flink.sustainability.NYT.types;

import com.flink.sustainability.NYT.function.*;


public class NYTEmptyTaxiReport {
    public long windowEnd;
    public String medallion;
    public int dropOffCellWE;
    public int dropOffCellNS;

    public NYTEmptyTaxiReport() {}

    public NYTEmptyTaxiReport(long windowEnd, String medallion, int dropOffCellWE, int dropOffCellNS) {
        this.windowEnd = windowEnd;
        this.medallion = medallion;
        this.dropOffCellWE = dropOffCellWE;
        this.dropOffCellNS = dropOffCellNS;
    }

    @Override
    public String toString() {
        return "NYTEmptyTaxiReport{" +
                "windowEnd=" + windowEnd +
                ", medallion='" + medallion + '\'' +
                ", dropOffCellWE=" + dropOffCellWE +
                ", dropOffCellNS=" + dropOffCellNS +
                '}';
    }
}
