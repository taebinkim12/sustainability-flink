package com.flink.sustainability.NYT;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class NYTProfitJoiner implements
        FlatJoinFunction<
                NYTProfitReport,
                NYTEmptyTaxiCountReport,
                NYTProfitabilityReport> {

    @Override
    public void join(
            NYTProfitReport first,
            NYTEmptyTaxiCountReport second,
            Collector<NYTProfitabilityReport> out) throws Exception {
        if (first.windowEnd != second.windowEnd) {
            throw new RuntimeException("Mismatching window IDs: " + first.windowEnd + " != " + second.windowEnd);
        }

        if (second.count == 0) {
            return;
        }

        double profitability = first.profit / second.count;

        out.collect(new NYTProfitabilityReport(first.windowEnd, first.cellIdWE, first.cellIdNS, profitability));
    }

    public static class ProfitJoinKey implements KeySelector<NYTProfitReport, Integer> {
        @Override
        public Integer getKey(NYTProfitReport value) throws Exception {
            return (value.cellIdWE << 16) | (value.cellIdNS & 0xFFFF);
        }
    }

    public static class EmptyTaxisJoinKey implements KeySelector<NYTEmptyTaxiCountReport, Integer> {
        @Override
        public Integer getKey(NYTEmptyTaxiCountReport value) throws Exception {
            return (value.cellIdWE << 16) | (value.cellIdNS & 0xFFFF);
        }
    }
}
