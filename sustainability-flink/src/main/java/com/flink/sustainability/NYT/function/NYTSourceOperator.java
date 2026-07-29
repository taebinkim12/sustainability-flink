package com.flink.sustainability.NYT.function;

import com.flink.sustainability.NYT.types.*;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class NYTSourceOperator extends RichParallelSourceFunction<NYTEventCondensed> {
    private String filePath;
    private int cacheSize;
    private int eventsPerSec;
    private int localEventsPerSec;
    private long durationSec;
    private String throughputFilePrefix;

    private transient List<NYTEventCondensed> eventCache;
    private volatile boolean isRunning = true;

    private transient List<Double> throughputSamples;
    private transient boolean throughputReported;
    private transient long totalEventsEmitted;

    public NYTSourceOperator(String filePath, int cacheSize, int eventsPerSec, long durationSec, String throughputFilePrefix) {
        this.filePath = filePath;
        this.cacheSize = cacheSize;
        this.eventsPerSec = eventsPerSec;
        this.durationSec = durationSec;
        this.throughputFilePrefix = throughputFilePrefix;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        eventCache = new ArrayList<>(cacheSize);
        throughputSamples = new ArrayList<>();
        throughputReported = false;
        
        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        this.localEventsPerSec = this.eventsPerSec / parallelism;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            long lineIndex = 0;
            long startLine = (long) cacheSize * subtaskIdx;
            long endLine = (long) cacheSize * (subtaskIdx + 1);

            while ((line = reader.readLine()) != null && eventCache.size() < cacheSize) {
                // Partition reading by contiguous blocks for each instance
                if (lineIndex >= startLine && lineIndex < endLine) {
                    NYTEventCondensed event = NYTEventCondensed.parseLine(line);
                    if (event != null) {
                        eventCache.add(event);
                    }
                } else if (lineIndex >= endLine) {
                    break; // Stop reading once we pass our designated block
                }
                lineIndex++;
            }
        }
        System.out.println("Subtask " + subtaskIdx + " done file reading. Loaded " + eventCache.size() + " events.");
    }

    @Override
    public void run(SourceContext<NYTEventCondensed> ctx) throws Exception {
        if (eventCache == null || eventCache.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        long durationMs = durationSec * 1000L;
        totalEventsEmitted = 0;
        long lastSnapshotTime = startTime;
        long lastSnapshotCount = 0;
        int cacheIdx = 0;
        int actualCacheSize = eventCache.size();

        long nanosPerEvent = localEventsPerSec > 0 ? 1_000_000_000L / localEventsPerSec : 0;
        long nextEmissionTime = System.nanoTime();

        while (isRunning && (System.currentTimeMillis() - startTime < durationMs)) {
            long currentTime = System.currentTimeMillis();
            
            // Check if 1 second has elapsed since the last snapshot
            if (currentTime - lastSnapshotTime >= 1000) {
                long actualIntervalMs = currentTime - lastSnapshotTime;
                long diff = totalEventsEmitted - lastSnapshotCount;
                
                // Calculate true events per second for this irregular window
                double trueRate = (diff / (double) actualIntervalMs) * 1000.0;
                throughputSamples.add(trueRate);
                
                lastSnapshotCount = totalEventsEmitted;
                lastSnapshotTime = currentTime;
            }
            
            if (nanosPerEvent > 0) {
                long currentNano = System.nanoTime();
                if (currentNano < nextEmissionTime) {
                    java.util.concurrent.locks.LockSupport.parkNanos(nextEmissionTime - currentNano);
                }
                // Schedule the next event relative to NOW, preventing "banking" of missed events
                nextEmissionTime = System.nanoTime() + nanosPerEvent;
            }

            ctx.collect(eventCache.get(cacheIdx));
            cacheIdx = (cacheIdx + 1) % actualCacheSize;
            totalEventsEmitted++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        reportThroughput();
    }

    @Override
    public void close() throws Exception {
        super.close();
        reportThroughput();
    }

    private synchronized void reportThroughput() {
        if (throughputReported) return;
        throughputReported = true;

        if (throughputSamples != null) {
            double sum = 0;
            int count = 0;

            if (throughputSamples.size() >= 3) {
                // Drop first and last
                for (int i = 1; i < throughputSamples.size() - 1; i++) {
                    sum += throughputSamples.get(i);
                    count++;
                }
            } else {
                // Just average what we have
                for (Double sample : throughputSamples) {
                    sum += sample;
                    count++;
                }
            }

            double averageThroughput = count > 0 ? sum / count : 0.0;

            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String fileName = throughputFilePrefix + "_subtask_" + subtaskIdx + ".csv";

            try (PrintWriter writer = new PrintWriter(new FileWriter(fileName, true))) {
                writer.println("--- Subtask " + subtaskIdx + " Throughput Samples (events/sec) ---");
                for (int i = 0; i < throughputSamples.size(); i++) {
                    writer.println("Sample " + (i + 1) + ": " + throughputSamples.get(i));
                }
                writer.println("-----------------------------------------------------");
                writer.println("Subtask " + subtaskIdx + " Steady-State Average Throughput (events/sec): " + averageThroughput);
                writer.println("Subtask " + subtaskIdx + " Total Events Emitted: " + totalEventsEmitted);
                
                System.out.println("Subtask " + subtaskIdx + " Steady-State Average Throughput: " + averageThroughput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
