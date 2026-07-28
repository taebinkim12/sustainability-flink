package com.flink.sustainability.NYT;

import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NYTProfitFunction implements
        WindowFunction<
                NYTEventProjected,
                NYTProfitReport,
                Tuple2<Integer, Integer>,
                TimeWindow> {

    @Override
    public void apply(
            Tuple2<Integer, Integer> cellId,
            TimeWindow window,
            Iterable<NYTEventProjected> values,
            Collector<NYTProfitReport> out) throws Exception {
        List<Double> faretip = new ArrayList<>();
        for (NYTEventProjected tr : values) {
            faretip.add(tr.fareAmount + tr.tipAmount);
        }

        if (faretip.isEmpty()) {
            return;
        }

        double[] gains = new double[faretip.size()];
        for (int i = 0; i < gains.length; i++) {
            gains[i] = faretip.get(i);
        }

        Arrays.sort(gains);

        double res = (new Median()).evaluate(gains);

        out.collect(new NYTProfitReport(window.getEnd(), cellId.f0, cellId.f1, res));
    }
}
