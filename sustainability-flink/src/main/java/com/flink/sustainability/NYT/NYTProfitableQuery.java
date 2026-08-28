package com.flink.sustainability.NYT;

import com.flink.sustainability.NYT.types.*;
import com.flink.sustainability.NYT.function.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class NYTProfitableQuery {

    private static final long K_WINDOW_DURATION_BASE_SECONDS = 10;
    private static final long SLIDING_WINDOW_DURATION_SECONDS = K_WINDOW_DURATION_BASE_SECONDS * 15;

    public static void main(String[] args) throws Exception {
        QueryConfig config = QueryConfig.create(args);

        for (int i = 0; i < config.numQueries; i++) {
            String querySsg = "query_group_" + i;
            String queryPrefix = config.throughputFilePrefix + "_q" + i;

            // Incorporate the source operator with parsed arguments
            SingleOutputStreamOperator<NYTEventCondensed> events = config.env.addSource(new NYTSourceOperator(
                config.inputFile,
                config.cacheSize,
                config.throughputPerQuery,
                config.duration,
                queryPrefix
            ));
            if (config.isolateQueries) {
                events = events.slotSharingGroup(querySsg).setParallelism(1);
            }

            SingleOutputStreamOperator<NYTEventProjected> projectedEvents = events.map(e -> new NYTEventProjected(
                e.medallion, e.pickupCellWE, e.pickupCellNS,
                e.dropOffCellWE, e.dropOffCellNS, e.fareAmount, e.tipAmount
            ));
            if (config.isolateQueries) {
                projectedEvents = projectedEvents.slotSharingGroup(querySsg).setParallelism(1);
            }

            // Assign timestamps based on system clock (ingestion time)
            SingleOutputStreamOperator<NYTEventProjected> rides = projectedEvents.assignTimestampsAndWatermarks(
                WatermarkStrategy.<NYTEventProjected>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
            );
            if (config.isolateQueries) {
                rides = rides.slotSharingGroup(querySsg).setParallelism(1);
            }

            // PROFIT
            SingleOutputStreamOperator<NYTProfitReport> profit = rides
                .keyBy(value -> (value.pickupCellWE << 16) | (value.pickupCellNS & 0xFFFF))
                .timeWindow(
                    Time.of(SLIDING_WINDOW_DURATION_SECONDS, TimeUnit.SECONDS),
                    Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new NYTProfitFunction());
            if (config.isolateQueries) {
                profit = profit.slotSharingGroup(querySsg).setParallelism(1);
            }
            profit = profit.assignTimestampsAndWatermarks(WatermarkStrategy.<NYTProfitReport>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.windowEnd));
            if (config.isolateQueries) {
                profit = profit.slotSharingGroup(querySsg).setParallelism(1);
            }

            // EMPTY TAXIS
            SingleOutputStreamOperator<NYTEmptyTaxiReport> emptyTaxis = rides
                .keyBy(value -> value.medallion)
                .timeWindow(
                    Time.of(SLIDING_WINDOW_DURATION_SECONDS, TimeUnit.SECONDS),
                    Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new NYTEmptyTaxiFunction());
            if (config.isolateQueries) {
                emptyTaxis = emptyTaxis.slotSharingGroup(querySsg).setParallelism(1);
            }
            emptyTaxis = emptyTaxis.assignTimestampsAndWatermarks(WatermarkStrategy.<NYTEmptyTaxiReport>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.windowEnd));
            if (config.isolateQueries) {
                emptyTaxis = emptyTaxis.slotSharingGroup(querySsg).setParallelism(1);
            }

            // EMPTY TAXIS COUNTER
            SingleOutputStreamOperator<NYTEmptyTaxiCountReport> emptyTaxisCount = emptyTaxis
                .keyBy(value -> (value.dropOffCellWE << 16) | (value.dropOffCellNS & 0xFFFF))
                .timeWindow(Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS))
                .apply(new NYTEmptyTaxisCounter());
            if (config.isolateQueries) {
                emptyTaxisCount = emptyTaxisCount.slotSharingGroup(querySsg).setParallelism(1);
            }
            emptyTaxisCount = emptyTaxisCount.assignTimestampsAndWatermarks(WatermarkStrategy.<NYTEmptyTaxiCountReport>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.windowEnd));
            if (config.isolateQueries) {
                emptyTaxisCount = emptyTaxisCount.slotSharingGroup(querySsg).setParallelism(1);
            }

            // PROFITABILITY
            SingleOutputStreamOperator<NYTProfitabilityReport> profitability = (SingleOutputStreamOperator<NYTProfitabilityReport>) profit
                .join(emptyTaxisCount)
                .where(new NYTProfitJoiner.ProfitJoinKey())
                .equalTo(new NYTProfitJoiner.EmptyTaxisJoinKey())
                .window(TumblingEventTimeWindows.of(Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)))
                .apply(new NYTProfitJoiner());
            if (config.isolateQueries) {
                profitability = profitability.slotSharingGroup(querySsg).setParallelism(1);
            }

            // Output results
            var sink = profitability.addSink(new DiscardingSink<>());
            if (config.isolateQueries) {
                sink.slotSharingGroup(querySsg).setParallelism(1);
            }
        }

        // Execute program, beginning computation.
        config.env.execute("NYT Profitable Query Job");
    }
}
