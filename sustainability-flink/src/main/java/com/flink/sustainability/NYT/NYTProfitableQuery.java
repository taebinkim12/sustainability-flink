package com.flink.sustainability.NYT;

import com.flink.sustainability.NYT.types.*;
import com.flink.sustainability.NYT.function.*;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class NYTProfitableQuery {

    private static final long K_WINDOW_DURATION_BASE_SECONDS = 10;
    private static final long SLIDING_WINDOW_DURATION_SECONDS = K_WINDOW_DURATION_BASE_SECONDS * 15;

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        ParameterTool parameters = ParameterTool.fromArgs(args);
        
        String inputFile = parameters.get("input-file", "~/NYT-data/2013_header_less_sorted.csv");
        int cacheSize = parameters.getInt("cache-size", 100000);
        int throughput = parameters.getInt("throughput", 100000);
        long duration = parameters.getLong("duration", 180);
        String throughputFilePrefix = parameters.get("throughput-file-prefix", "throughput_results");

        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        // Network memory min and max MUST match exactly when running via MiniCluster
        conf.setString("taskmanager.memory.network.min", "2gb");
        conf.setString("taskmanager.memory.network.max", "2gb");
        conf.setString("taskmanager.memory.network.fraction", "0.2");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        
        int numQueries = parameters.getInt("num-queries", 1);
        int throughputPerQuery = throughput;

        for (int i = 0; i < numQueries; i++) {
            String querySsg = "query_group_" + i;
            String queryPrefix = throughputFilePrefix + "_q" + i;

            // Incorporate the source operator with parsed arguments
            DataStream<NYTEventCondensed> events = env.addSource(new NYTSourceOperator(
                inputFile, 
                cacheSize, 
                throughputPerQuery, 
                duration,
                queryPrefix
            )).slotSharingGroup(querySsg).setParallelism(1);

            DataStream<NYTEventProjected> projectedEvents = events.map(e -> new NYTEventProjected(
                e.medallion, e.pickupCellWE, e.pickupCellNS, 
                e.dropOffCellWE, e.dropOffCellNS, e.fareAmount, e.tipAmount
            )).slotSharingGroup(querySsg).setParallelism(1);

            // Assign timestamps based on system clock (ingestion time)
            DataStream<NYTEventProjected> rides = projectedEvents.assignTimestampsAndWatermarks(
                WatermarkStrategy.<NYTEventProjected>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
            ).slotSharingGroup(querySsg).setParallelism(1);

            // PROFIT
            DataStream<NYTProfitReport> profit = rides
                .keyBy(value -> (value.pickupCellWE << 16) | (value.pickupCellNS & 0xFFFF))
                .timeWindow(
                    Time.of(SLIDING_WINDOW_DURATION_SECONDS, TimeUnit.SECONDS),
                    Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new NYTProfitFunction())
                .slotSharingGroup(querySsg).setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<NYTProfitReport>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> event.windowEnd))
                .slotSharingGroup(querySsg).setParallelism(1);

            // EMPTY TAXIS
            DataStream<NYTEmptyTaxiReport> emptyTaxis = rides
                .keyBy(value -> value.medallion)
                .timeWindow(
                    Time.of(SLIDING_WINDOW_DURATION_SECONDS, TimeUnit.SECONDS),
                    Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new NYTEmptyTaxiFunction())
                .slotSharingGroup(querySsg).setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<NYTEmptyTaxiReport>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> event.windowEnd))
                .slotSharingGroup(querySsg).setParallelism(1);

            // EMPTY TAXIS COUNTER
            DataStream<NYTEmptyTaxiCountReport> emptyTaxisCount = emptyTaxis
                .keyBy(value -> (value.dropOffCellWE << 16) | (value.dropOffCellNS & 0xFFFF))
                .timeWindow(Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS))
                .apply(new NYTEmptyTaxisCounter())
                .slotSharingGroup(querySsg).setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<NYTEmptyTaxiCountReport>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> event.windowEnd))
                .slotSharingGroup(querySsg).setParallelism(1);

            // PROFITABILITY
            SingleOutputStreamOperator<NYTProfitabilityReport> profitability = ((SingleOutputStreamOperator<NYTProfitabilityReport>) profit
                .join(emptyTaxisCount)
                .where(new NYTProfitJoiner.ProfitJoinKey())
                .equalTo(new NYTProfitJoiner.EmptyTaxisJoinKey())
                .window(TumblingEventTimeWindows.of(Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)))
                .apply(new NYTProfitJoiner()))
                .slotSharingGroup(querySsg).setParallelism(1);

            // Output results
            profitability.addSink(new DiscardingSink<>())
                .slotSharingGroup(querySsg).setParallelism(1);
        }

        // Execute program, beginning computation.
        env.execute("NYT Profitable Query Job");
    }
}
