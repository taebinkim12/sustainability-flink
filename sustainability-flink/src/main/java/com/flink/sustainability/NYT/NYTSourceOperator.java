package com.flink.sustainability.NYT;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NYTSourceOperator extends RichParallelSourceFunction<NYTEventCondensed> {
    private String filePath;
    private int cacheSize;
    private int eventsPerSec;
    private long durationSec;
    private String throughputFilePrefix;

    private transient List<NYTEventCondensed> eventCache;
    private volatile boolean isRunning = true;

    private transient ScheduledExecutorService scheduler;
    private transient AtomicLong emittedEvents;
    private transient long lastEmittedCount;
    private transient List<Long> throughputSamples;
    private transient boolean throughputReported;

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
        emittedEvents = new AtomicLong(0);
        lastEmittedCount = 0;
        throughputSamples = new ArrayList<>();
        throughputReported = false;

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long currentCount = emittedEvents.get();
            long diff = currentCount - lastEmittedCount;
            lastEmittedCount = currentCount;
            throughputSamples.add(diff);
        }, 1, 1, TimeUnit.SECONDS);
        
        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

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
        int cacheIdx = 0;
        int actualCacheSize = eventCache.size();

        while (isRunning && (System.currentTimeMillis() - startTime < durationMs)) {
            long currentTime = System.currentTimeMillis();
            long elapsedMs = currentTime - startTime;
            
            // Expected number of events that should have been emitted by now
            long expectedCount = (long) ((elapsedMs / 1000.0) * eventsPerSec);

            if (emittedEvents.get() < expectedCount) {
                ctx.collect(eventCache.get(cacheIdx));
                cacheIdx = (cacheIdx + 1) % actualCacheSize;
                emittedEvents.incrementAndGet();
            } else {
                // Yield to prevent 100% CPU busy-waiting
                java.util.concurrent.locks.LockSupport.parkNanos(10000L);
            }
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

        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        if (throughputSamples != null) {
            long sum = 0;
            int count = 0;

            if (throughputSamples.size() >= 3) {
                // Drop first and last
                for (int i = 1; i < throughputSamples.size() - 1; i++) {
                    sum += throughputSamples.get(i);
                    count++;
                }
            } else {
                // Just average what we have
                for (Long sample : throughputSamples) {
                    sum += sample;
                    count++;
                }
            }

            double averageThroughput = count > 0 ? (double) sum / count : 0.0;

            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String fileName = throughputFilePrefix + "_subtask_" + subtaskIdx + ".csv";

            try (PrintWriter writer = new PrintWriter(new FileWriter(fileName, true))) {
                writer.println("Subtask " + subtaskIdx + " Steady-State Average Throughput (events/sec): " + averageThroughput);
                System.out.println("Subtask " + subtaskIdx + " Steady-State Average Throughput: " + averageThroughput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
