package com.flink.sustainability.NYT;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class NYTProfitableQuery {

    private static final long K_WINDOW_DURATION_BASE_SECONDS = 10;
    private static final long SLIDING_WINDOW_DURATION_SECONDS = K_WINDOW_DURATION_BASE_SECONDS * 15;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Increase network buffers to support high parallelism on remote machines with many cores
        conf.setString("taskmanager.memory.network.min", "256m");
        conf.setString("taskmanager.memory.network.max", "1g");
        conf.setString("taskmanager.memory.network.fraction", "0.2");
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // Parse command line arguments
        ParameterTool parameters = ParameterTool.fromArgs(args);
        
        String inputFile = parameters.get("input-file", "~/NYT-data/2013_header_less_sorted.csv");
        int cacheSize = parameters.getInt("cache-size", 100000);
        int throughput = parameters.getInt("throughput", 100000);
        long duration = parameters.getLong("duration", 180);

        // Incorporate the source operator with parsed arguments
        DataStream<NYTEventCondensed> events = env.addSource(new NYTSourceOperator(
            inputFile, 
            cacheSize, 
            throughput, 
            duration
        ));

        DataStream<NYTEventProjected> projectedEvents = events.map(e -> new NYTEventProjected(
            e.medallion, e.pickupCellWE, e.pickupCellNS, 
            e.dropOffCellWE, e.dropOffCellNS, e.fareAmount, e.tipAmount
        ));

        // Assign timestamps based on system clock (ingestion time)
        DataStream<NYTEventProjected> rides = projectedEvents.assignTimestampsAndWatermarks(
            WatermarkStrategy.<NYTEventProjected>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
        );

        // PROFIT
        DataStream<NYTProfitReport> profit = rides
            .keyBy(
                new KeySelector<NYTEventProjected, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(NYTEventProjected value) throws Exception {
                        return Tuple2.of(value.pickupCellWE, value.pickupCellNS);
                    }
                })
            .timeWindow(
                Time.of(SLIDING_WINDOW_DURATION_SECONDS, TimeUnit.SECONDS),
                Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)
            )
            .apply(new NYTProfitFunction())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<NYTProfitReport>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.windowEnd));

        // EMPTY TAXIS
        DataStream<NYTEmptyTaxiReport> emptyTaxis = rides
            .keyBy(
                new KeySelector<NYTEventProjected, String>() {
                    @Override
                    public String getKey(NYTEventProjected value) throws Exception {
                        return value.medallion;
                    }
                })
            .timeWindow(
                Time.of(SLIDING_WINDOW_DURATION_SECONDS, TimeUnit.SECONDS),
                Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)
            )
            .apply(new NYTEmptyTaxiFunction())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<NYTEmptyTaxiReport>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.windowEnd));

        // EMPTY TAXIS COUNTER
        DataStream<NYTEmptyTaxiCountReport> emptyTaxisCount = emptyTaxis
            .keyBy(new KeySelector<NYTEmptyTaxiReport, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> getKey(NYTEmptyTaxiReport value) throws Exception {
                    return Tuple2.of(value.dropOffCellWE, value.dropOffCellNS);
                }
            })
            .timeWindow(Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS))
            .apply(new NYTEmptyTaxisCounter())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<NYTEmptyTaxiCountReport>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.windowEnd));

        // PROFITABILITY
        DataStream<NYTProfitabilityReport> profitability = profit
            .join(emptyTaxisCount)
            .where(new NYTProfitJoiner.ProfitJoinKey())
            .equalTo(new NYTProfitJoiner.EmptyTaxisJoinKey())
            .window(TumblingEventTimeWindows.of(Time.of(K_WINDOW_DURATION_BASE_SECONDS, TimeUnit.SECONDS)))
            .apply(new NYTProfitJoiner());

        // Output results
        profitability.addSink(new PrintSinkFunction<>());

        // Execute program, beginning computation.
        env.execute("NYT Profitable Query Job");
    }
}
