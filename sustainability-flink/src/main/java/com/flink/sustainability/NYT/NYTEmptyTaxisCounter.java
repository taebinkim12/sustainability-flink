package com.flink.sustainability.NYT;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class NYTEmptyTaxisCounter implements
        WindowFunction<
                NYTEmptyTaxiReport,
                NYTEmptyTaxiCountReport,
                Tuple2<Integer, Integer>, TimeWindow> {

    @Override
    public void apply(
            Tuple2<Integer, Integer> cellId, TimeWindow window,
            Iterable<NYTEmptyTaxiReport> values,
            Collector<NYTEmptyTaxiCountReport> out) throws Exception {
        // removing duplicates
        Set<String> taxis = new HashSet<>();
        // checking for windowID correctness
        long lastWID = -1L;

        for (NYTEmptyTaxiReport t : values) {
            taxis.add(t.medallion);

            if (lastWID != -1L && t.windowEnd != lastWID) {
                throw new RuntimeException("Wrong windowID in this window: " + t.windowEnd + " != " + lastWID);
            }

            lastWID = t.windowEnd;
        }

        out.collect(new NYTEmptyTaxiCountReport(window.getEnd(), cellId.f0, cellId.f1, taxis.size()));
    }
}
