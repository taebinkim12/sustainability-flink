#!/bin/bash

# run_experiment.sh
# Script to run the NYTProfitableQuery Flink job iterating through multiple configurations

# Default values
EXECUTION_MODES=()
CACHE_SIZES=()
THROUGHPUTS=()
INPUT_FILE="$HOME/NYT-data/2013_header_less_sorted.csv"
DURATION=600
QUERY="profitable"
LOCAL=""

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --query)
            QUERY="$2"; shift 2;;
        --local)
            LOCAL="$2"; shift 2;;
        --execution-mode)
            shift
            while [[ "$#" -gt 0 && ! "$1" == --* ]]; do
                EXECUTION_MODES+=("$1")
                shift
            done
            ;;
        --cache-size)
            shift
            while [[ "$#" -gt 0 && ! "$1" == --* ]]; do
                CACHE_SIZES+=("$1")
                shift
            done
            ;;
        --throughput)
            shift
            while [[ "$#" -gt 0 && ! "$1" == --* ]]; do
                THROUGHPUTS+=("$1")
                shift
            done
            ;;
        --input-file)
            INPUT_FILE="$2"; shift 2;;
        --duration)
            DURATION="$2"; shift 2;;
        *) 
            echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
done

if [ "$QUERY" != "profitable" ] && [ "$QUERY" != "distance" ]; then
    echo "Error: Unknown query option '$QUERY'. Valid options are: profitable, distance"
    exit 1
fi

if [ -n "$LOCAL" ] && [ "$LOCAL" != "true" ] && [ "$LOCAL" != "false" ]; then
    echo "Error: Unknown --local option '$LOCAL'. Valid options are: true, false"
    exit 1
fi

if [ ${#EXECUTION_MODES[@]} -eq 0 ]; then
    EXECUTION_MODES=("single")
fi

if [ -z "$LOCAL" ]; then
    LOCAL="true"
    for mode in "${EXECUTION_MODES[@]}"; do
        if [ "$mode" = "distributed" ]; then
            LOCAL="false"
            break
        fi
    done
fi

for mode in "${EXECUTION_MODES[@]}"; do
    if [ "$mode" = "distributed" ] && [ "$LOCAL" = "true" ]; then
        echo "Error: Cannot run in distributed execution mode when --local is set to true. Stopping experiment."
        exit 1
    fi
done

if [ ${#THROUGHPUTS[@]} -eq 0 ]; then
    THROUGHPUTS=("0.01") # Default 10000 events/sec
fi
if [ ${#CACHE_SIZES[@]} -eq 0 ]; then
    CACHE_SIZES=(100000)
fi

echo "Building project and generating classpath..."
mvn -f sustainability-flink/pom.xml clean package -DskipTests
mvn -f sustainability-flink/pom.xml dependency:build-classpath -Dmdep.outputFile=classpath.txt

# Find the built JAR file
JAR_FILE=$(ls sustainability-flink/target/sustainability-flink-*.jar | head -n 1)

if [ -z "$JAR_FILE" ]; then
    echo "Error: Could not find the built JAR file in target/"
    exit 1
fi

# Create main directory for this batch of runs
MAIN_DIR=$(date +%Y%m%d_%H%M)
mkdir -p "$MAIN_DIR"
echo "Created main experiment directory: $MAIN_DIR"

run_job() {
    local MODE=$1
    local TPUT_MILLIONS=$2
    local IN_FILE=$3
    local CACHE_SIZE=$4

    # Calculate true throughput by multiplying by 1,000,000
    local TPUT=$(awk "BEGIN {print int($TPUT_MILLIONS * 1000000)}")

    # Create subfolder for this specific run
    local SUB_DIR="${MAIN_DIR}/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$SUB_DIR"
    local PREFIX="${SUB_DIR}/throughput_results"

    # Select main class name based on query
    local CLASS_NAME="com.flink.sustainability.NYT.NYTProfitableQuery"
    if [ "$QUERY" = "distance" ]; then
        CLASS_NAME="com.flink.sustainability.NYT.NYTDistanceQuery"
    fi

    echo ""
    echo "========================================================="
    echo "RUN CONFIGURATION:"
    echo "  Query:               $QUERY"
    echo "  Execution Mode:      $MODE"
    echo "  Input File:          $IN_FILE"
    echo "  Duration:            $DURATION seconds"
    echo "  Cache Size:          $CACHE_SIZE"
    echo "  Global Throughput:   $TPUT ($TPUT_MILLIONS M events/sec)"
    echo "  Output Directory:    $SUB_DIR"
    echo "========================================================="

    local CONFIG_FILE="${SUB_DIR}/config.txt"
    {
        echo "Query: $QUERY"
        echo "Execution Mode: $MODE"
        echo "Input File: $IN_FILE"
        echo "Duration: $DURATION"
        echo "Cache Size: $CACHE_SIZE"
        echo "Global Throughput: $TPUT"
    } > "$CONFIG_FILE"

    if [ "$LOCAL" = "false" ]; then
        if [ "$MODE" = "distributed" ]; then
            echo "Submitting job to Flink cluster..."
            flink run -c "$CLASS_NAME" "$JAR_FILE" \
                --input-file "$IN_FILE" \
                --cache-size "$CACHE_SIZE" \
                --throughput "$TPUT" \
                --duration "$DURATION" \
                --throughput-file-prefix "$PREFIX"
        else
            echo "Starting powerstat in background for single-node execution..."
            sudo powerstat -tfcRD 1 27000 > "${SUB_DIR}/powerstat_output.txt" 2>&1 &
            
            echo "Waiting 10 seconds to collect idle power usage before run..."
            sleep 10

            echo "Submitting job locally via Flink Standalone Cluster..."
            flink run -c "$CLASS_NAME" "$JAR_FILE" \
                --input-file "$IN_FILE" \
                --cache-size "$CACHE_SIZE" \
                --throughput "$TPUT" \
                --duration "$DURATION" \
                --throughput-file-prefix "$PREFIX"

            echo "Waiting 10 seconds to collect idle power usage after run..."
            sleep 10
            
            echo "Killing powerstat process..."
            sudo pkill -INT -f powerstat
        fi
    else
        echo "Starting powerstat in background for single-node execution..."
        sudo powerstat -tfcRD 1 27000 > "${SUB_DIR}/powerstat_output.txt" 2>&1 &
        
        echo "Waiting 10 seconds to collect idle power usage before run..."
        sleep 10

        echo "Running job locally via embedded MiniCluster..."
        CLASSPATH="$JAR_FILE:$(cat sustainability-flink/classpath.txt)"
        java \
            --add-opens=java.base/java.lang=ALL-UNNAMED \
            --add-opens=java.base/java.util=ALL-UNNAMED \
            --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
            --add-opens=java.base/java.io=ALL-UNNAMED \
            --add-opens=java.base/java.nio=ALL-UNNAMED \
            -cp "$CLASSPATH" "$CLASS_NAME" \
            --input-file "$IN_FILE" \
            --cache-size "$CACHE_SIZE" \
            --throughput "$TPUT" \
            --duration "$DURATION" \
            --throughput-file-prefix "$PREFIX"
            
        echo "Waiting 10 seconds to collect idle power usage after run..."
        sleep 10
        
        echo "Killing powerstat process..."
        sudo pkill -INT -f powerstat
    fi
}

for cache in "${CACHE_SIZES[@]}"; do
    for tput in "${THROUGHPUTS[@]}"; do
        for mode in "${EXECUTION_MODES[@]}"; do
            run_job "$mode" "$tput" "$INPUT_FILE" "$cache"
            echo "Job finished. Sleeping 5 seconds before next run..."
            sleep 5
        done
    done
done

echo "All experiments completed!"

# Summarize throughput
python3 summarize_throughput.py "$MAIN_DIR"
