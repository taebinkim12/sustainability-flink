package com.flink.sustainability.NYT.function;

import com.flink.sustainability.NYT.types.NYTDQEvent;
import com.flink.sustainability.NYT.types.NYTDistanceReport;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class NYTDistanceWindowFunction implements
        WindowFunction<
                NYTDQEvent,
                NYTDistanceReport,
                Integer,
                TimeWindow> {

    @Override
    public void apply(
            Integer cellId,
            TimeWindow window,
            Iterable<NYTDQEvent> values,
            Collector<NYTDistanceReport> out) throws Exception {
        long count = 0;
        double totalDistance = 0.0;

        for (NYTDQEvent event : values) {
            count++;
            totalDistance += event.tripDistance;
        }

        if (count > 0) {
            double avgTripDistance = totalDistance / count;
            out.collect(new NYTDistanceReport(window.getEnd(), cellId, count, avgTripDistance));
        }
    }
}
