package com.flink.sustainability.NYT.function;

import com.flink.sustainability.NYT.types.NYTEventCondensed;

public class NYTSourceOperator extends AbstractNYTSourceOperator<NYTEventCondensed> {

    public NYTSourceOperator(String filePath, int cacheSize, int eventsPerSec,
                             long durationSec, String throughputFilePrefix) {
        super(filePath, cacheSize, eventsPerSec, durationSec, throughputFilePrefix);
    }

    @Override
    protected NYTEventCondensed parseLine(String line) {
        return NYTEventCondensed.parseLine(line);
    }
}
