#!/bin/bash

# run_experiment.sh
# Script to run the NYTProfitableQuery Flink job

# Default values
EXECUTION_MODE="single"
CACHE_SIZE=100000
THROUGHPUT=10000
INPUT_FILE="~/NYT-data/2013_header_less_sorted.csv"
DURATION=60

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --execution-mode) EXECUTION_MODE="$2"; shift ;;
        --cache-size) CACHE_SIZE="$2"; shift ;;
        --throughput) THROUGHPUT="$2"; shift ;;
        --input-file) INPUT_FILE="$2"; shift ;;
        --duration) DURATION="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Calculate per-node throughput
PER_NODE_THROUGHPUT=$THROUGHPUT
if [ "$EXECUTION_MODE" = "distributed" ]; then
    PER_NODE_THROUGHPUT=$((THROUGHPUT / 2))
fi

echo "Building project..."
mvn -f sustainability-flink/pom.xml clean package -DskipTests

echo "Running NYTProfitableQuery in $EXECUTION_MODE mode..."
echo "Overall Throughput: $THROUGHPUT, Per-node Throughput: $PER_NODE_THROUGHPUT"
echo "Cache Size: $CACHE_SIZE, Duration: $DURATION s, Input File: $INPUT_FILE"

# Find the built JAR file
JAR_FILE=$(ls sustainability-flink/target/sustainability-flink-*.jar | head -n 1)

if [ -z "$JAR_FILE" ]; then
    echo "Error: Could not find the built JAR file in target/"
    exit 1
fi

# Submit the job to Flink (passing arguments to the Flink job)
flink run -c com.flink.sustainability.NYT.NYTProfitableQuery "$JAR_FILE" \
    --input-file "$INPUT_FILE" \
    --cache-size "$CACHE_SIZE" \
    --throughput "$PER_NODE_THROUGHPUT" \
    --duration "$DURATION"
