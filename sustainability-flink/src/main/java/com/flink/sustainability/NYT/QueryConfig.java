package com.flink.sustainability.NYT;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Shared configuration and environment setup used by both
 * {@link NYTProfitableQuery} and {@link NYTDistanceQuery}.
 */
public class QueryConfig {
    public final String inputFile;
    public final int cacheSize;
    public final int throughput;
    public final long duration;
    public final String throughputFilePrefix;
    public final int numQueries;
    public final boolean isolateQueries;
    public final int throughputPerQuery;
    public final StreamExecutionEnvironment env;

    private QueryConfig(ParameterTool parameters, StreamExecutionEnvironment env) {
        this.inputFile = parameters.get("input-file", "~/NYT-data/2013_header_less_sorted.csv");
        this.cacheSize = parameters.getInt("cache-size", 100000);
        this.throughput = parameters.getInt("throughput", 100000);
        this.duration = parameters.getLong("duration", 180);
        this.throughputFilePrefix = parameters.get("throughput-file-prefix", "throughput_results");
        this.numQueries = parameters.getInt("num-queries", 1);
        this.isolateQueries = parameters.getBoolean("isolate-queries", true);
        this.throughputPerQuery = this.throughput;
        this.env = env;
    }

    /**
     * Parse CLI arguments, configure the Flink environment with network memory
     * settings, and return a fully-initialized QueryConfig.
     */
    public static QueryConfig create(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        Configuration conf = new Configuration();
        // Network memory min and max MUST match exactly when running via MiniCluster
        conf.setString("taskmanager.memory.network.min", "2gb");
        conf.setString("taskmanager.memory.network.max", "2gb");
        conf.setString("taskmanager.memory.network.fraction", "0.2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        return new QueryConfig(parameters, env);
    }
}
