package com.flink.sustainability.NYT;

import com.flink.sustainability.NYT.types.*;
import com.flink.sustainability.NYT.function.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;

public class NYTDistanceQuery {

    private static final long WINDOW_DURATION_SECONDS = 2;

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
        
        // Cap parallelism to prevent single JVM from spinning up 15,000+ threads on massive servers
        if (env.getParallelism() > 128) {
            env.setParallelism(128);
        }

        // Incorporate the source operator with parsed arguments
        DataStream<NYTDQEvent> events = env.addSource(new NYTDQSourceOperator(
            inputFile, 
            cacheSize, 
            throughput, 
            duration,
            throughputFilePrefix
        ));

        // Assign timestamps based on system clock (ingestion time)
        DataStream<NYTDQEvent> rides = events.assignTimestampsAndWatermarks(
            WatermarkStrategy.<NYTDQEvent>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
        );

        // Filter: dropoffCell > 0 && pickupCell > 0 && vendorId is "VTS" && tripDistance > 1
        DataStream<NYTDQEvent> filteredRides = rides.filter(event -> 
            event.dropoffCell > 0 && 
            event.pickupCell > 0 && 
            event.vendorId != null && 
            event.vendorId.equals("VTS") && 
            event.tripDistance > 1
        );

        // Window & Average Distance aggregation
        DataStream<NYTDistanceReport> distanceReports = filteredRides
            .keyBy(value -> value.pickupCell)
            .timeWindow(Time.seconds(WINDOW_DURATION_SECONDS))
            .apply(new NYTDistanceWindowFunction());

        // Output results
        distanceReports.addSink(new DiscardingSink<>());

        // Execute program, beginning computation.
        env.execute("NYT Distance Query Job");
    }
}
