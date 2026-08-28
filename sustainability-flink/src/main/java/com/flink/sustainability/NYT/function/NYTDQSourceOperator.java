package com.flink.sustainability.NYT.function;

import com.flink.sustainability.NYT.types.NYTDQEvent;

public class NYTDQSourceOperator extends AbstractNYTSourceOperator<NYTDQEvent> {

    public NYTDQSourceOperator(String filePath, int cacheSize, int eventsPerSec,
                               long durationSec, String throughputFilePrefix) {
        super(filePath, cacheSize, eventsPerSec, durationSec, throughputFilePrefix);
    }

    @Override
    protected NYTDQEvent parseLine(String line) {
        return NYTDQEvent.parseLine(line);
    }
}
