package com.flink.sustainability.NYT;

import com.flink.sustainability.NYT.types.*;
import com.flink.sustainability.NYT.function.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;

public class NYTDistanceQuery {

    private static final long WINDOW_DURATION_SECONDS = 2;

    public static void main(String[] args) throws Exception {
        QueryConfig config = QueryConfig.create(args);

        for (int i = 0; i < config.numQueries; i++) {
            String querySsg = "query_group_" + i;
            String queryPrefix = config.throughputFilePrefix + "_q" + i;

            // Incorporate the source operator with parsed arguments
            SingleOutputStreamOperator<NYTDQEvent> events = config.env.addSource(new NYTDQSourceOperator(
                config.inputFile,
                config.cacheSize,
                config.throughputPerQuery,
                config.duration,
                queryPrefix
            ));
            if (config.isolateQueries) {
                events = events.slotSharingGroup(querySsg).setParallelism(1);
            }

            // Assign timestamps based on system clock (ingestion time)
            SingleOutputStreamOperator<NYTDQEvent> rides = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<NYTDQEvent>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
            );
            if (config.isolateQueries) {
                rides = rides.slotSharingGroup(querySsg).setParallelism(1);
            }

            // Filter: dropoffCell > 0 && pickupCell > 0 && vendorId is "VTS" && tripDistance > 1
            SingleOutputStreamOperator<NYTDQEvent> filteredRides = rides.filter(event ->
                event.dropoffCell > 0 &&
                event.pickupCell > 0 &&
                event.vendorId != null &&
                event.vendorId.equals("VTS") &&
                event.tripDistance > 1
            );
            if (config.isolateQueries) {
                filteredRides = filteredRides.slotSharingGroup(querySsg).setParallelism(1);
            }

            // Window & Average Distance aggregation
            SingleOutputStreamOperator<NYTDistanceReport> distanceReports = filteredRides
                .keyBy(value -> value.pickupCell)
                .timeWindow(Time.seconds(WINDOW_DURATION_SECONDS))
                .apply(new NYTDistanceWindowFunction());
            if (config.isolateQueries) {
                distanceReports = distanceReports.slotSharingGroup(querySsg).setParallelism(1);
            }

            // Output results
            var sink = distanceReports.addSink(new DiscardingSink<>());
            if (config.isolateQueries) {
                sink.slotSharingGroup(querySsg).setParallelism(1);
            }
        }

        // Execute program, beginning computation.
        config.env.execute("NYT Distance Query Job");
    }
}
