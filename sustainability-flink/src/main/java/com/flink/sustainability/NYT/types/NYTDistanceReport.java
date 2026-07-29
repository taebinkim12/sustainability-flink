package com.flink.sustainability.NYT.types;

public class NYTDistanceReport {
    public long windowEnd;
    public int cellId;
    public long count;
    public double avgTripDistance;

    public NYTDistanceReport() {}

    public NYTDistanceReport(long windowEnd, int cellId, long count, double avgTripDistance) {
        this.windowEnd = windowEnd;
        this.cellId = cellId;
        this.count = count;
        this.avgTripDistance = avgTripDistance;
    }

    @Override
    public String toString() {
        return "NYTDistanceReport{" +
                "windowEnd=" + windowEnd +
                ", cellId=" + cellId +
                ", count=" + count +
                ", avgTripDistance=" + avgTripDistance +
                '}';
    }
}
