package com.flink.sustainability.NYT.function;

import com.flink.sustainability.NYT.types.*;


import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class NYTEmptyTaxiFunction implements
        WindowFunction<
                NYTEventProjected,
                NYTEmptyTaxiReport,
                String,
                TimeWindow> {

    @Override
    public void apply(
            String medallion,
            TimeWindow window,
            Iterable<NYTEventProjected> values,
            Collector<NYTEmptyTaxiReport> out) throws Exception {
        NYTEventProjected lastone = null;
        for (NYTEventProjected tr : values) {
            lastone = tr;
        }

        if (lastone != null) {
            out.collect(new NYTEmptyTaxiReport(window.getEnd(), medallion, lastone.dropOffCellWE, lastone.dropOffCellNS));
        }
    }
}
