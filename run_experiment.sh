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
NUM_QUERIES_LIST=()
REMOTE_HOSTS=()

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
        --num-queries)
            shift
            while [[ "$#" -gt 0 && ! "$1" == --* ]]; do
                NUM_QUERIES_LIST+=("$1")
                shift
            done
            ;;
        --remote-hosts)
            shift
            while [[ "$#" -gt 0 && ! "$1" == --* ]]; do
                REMOTE_HOSTS+=("$1")
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

if [ ${#NUM_QUERIES_LIST[@]} -eq 0 ]; then
    NUM_QUERIES_LIST=(4)
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

if ! command -v flink &> /dev/null; then
    if [ -d "$PWD/flink-1.18.0/bin" ]; then
        echo "[INFO] Flink not found in PATH. Adding local flink-1.18.0 to PATH."
        export PATH="$PWD/flink-1.18.0/bin:$PATH"
        FLINK_DIR="$PWD/flink-1.18.0"
    else
        echo "Error: flink command not found in PATH."
        exit 1
    fi
else
    FLINK_BIN_PATH=$(command -v flink)
    FLINK_DIR=$(dirname "$(dirname "$FLINK_BIN_PATH")")
fi

force_kill_flink() {
    echo "[INFO] Force killing any existing Flink cluster processes..."
    if [ -f "${FLINK_DIR}/bin/stop-cluster.sh" ]; then
        "${FLINK_DIR}/bin/stop-cluster.sh" >/dev/null 2>&1
    fi
    pkill -9 -f "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint" >/dev/null 2>&1
    pkill -9 -f "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" >/dev/null 2>&1
    rm -f /tmp/flink-*.pid >/dev/null 2>&1

    local WORKERS_FILE="${FLINK_DIR}/conf/workers"
    if [ -f "$WORKERS_FILE" ]; then
        while read -r worker || [ -n "$worker" ]; do
            if [[ -z "$worker" || "$worker" =~ ^# ]]; then
                continue
            fi
            echo "[INFO] Force killing Flink on remote worker: $worker"
            ssh -o ConnectTimeout=5 "$worker" "pkill -9 -f 'org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint' >/dev/null 2>&1; pkill -9 -f 'org.apache.flink.runtime.taskexecutor.TaskManagerRunner' >/dev/null 2>&1; rm -f /tmp/flink-*.pid >/dev/null 2>&1" >/dev/null 2>&1 &
        done < "$WORKERS_FILE"
        wait
    fi
}

# Kill any Flink cluster before we start the whole experiment
force_kill_flink

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
MAIN_DIR="$PWD/$(date +%Y%m%d_%H%M)"
mkdir -p "$MAIN_DIR"
echo "Created main experiment directory: $MAIN_DIR"

start_powerstat() {
    local SUB_DIR=$1
    local MODE=$2

    echo "[INFO] Starting powerstat on local host ($(hostname))..."
    sudo powerstat -tfcRD 1 27000 > "${SUB_DIR}/powerstat_output_$(hostname).txt" 2>&1 &
    LOCAL_POWERSTAT_PID=$!

    if [ "$MODE" = "distributed" ] && [ ${#REMOTE_HOSTS[@]} -gt 0 ]; then
        REMOTE_PIDS=()
        for host in "${REMOTE_HOSTS[@]}"; do
            echo "[INFO] Starting powerstat on remote host: $host"
            local rpid
            rpid=$(ssh -o ConnectTimeout=5 "$host" "sudo nohup powerstat -tfcRD 1 27000 > '${SUB_DIR}/powerstat_output_${host}.txt' 2>&1 & echo \\\$!")
            rpid=$(echo "$rpid" | tr -d '\r\n[:space:]')
            echo "[INFO] Remote powerstat PID on $host is $rpid"
            REMOTE_PIDS+=("$host:$rpid")
        done
    fi
}

stop_powerstat() {
    local MODE=$1
    echo "[INFO] Killing local powerstat process..."
    if [ -n "$LOCAL_POWERSTAT_PID" ]; then
        sudo kill -INT "$LOCAL_POWERSTAT_PID" >/dev/null 2>&1
        wait "$LOCAL_POWERSTAT_PID" 2>/dev/null
    else
        sudo pkill -INT -f powerstat >/dev/null 2>&1
    fi

    if [ "$MODE" = "distributed" ] && [ ${#REMOTE_PIDS[@]} -gt 0 ]; then
        for entry in "${REMOTE_PIDS[@]}"; do
            local host=${entry%%:*}
            local rpid=${entry##*:}
            if [ -n "$rpid" ]; then
                echo "[INFO] Killing remote powerstat process on $host (PID: $rpid)..."
                ssh -o ConnectTimeout=5 "$host" "sudo kill -INT $rpid" >/dev/null 2>&1
            fi
        done
        unset REMOTE_PIDS
    fi
}

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
    echo "  Number of Queries:   $NUM_QUERIES"
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
        echo "Number of Queries: $NUM_QUERIES"
        echo "Global Throughput: $TPUT"
    } > "$CONFIG_FILE"

    start_powerstat "$SUB_DIR" "$MODE"

    echo "Waiting 10 seconds to collect idle power usage before run..."
    sleep 10

    if [ "$LOCAL" = "false" ]; then
        if [ "$MODE" = "distributed" ]; then
            echo "Submitting job to Flink cluster..."
            flink run -c "$CLASS_NAME" "$JAR_FILE" \
                --input-file "$IN_FILE" \
                --cache-size "$CACHE_SIZE" \
                --throughput "$TPUT" \
                --duration "$DURATION" \
                --throughput-file-prefix "$PREFIX" \
                --num-queries "$NUM_QUERIES"
        else
            echo "Submitting job locally via Flink Standalone Cluster..."
            flink run -c "$CLASS_NAME" "$JAR_FILE" \
                --input-file "$IN_FILE" \
                --cache-size "$CACHE_SIZE" \
                --throughput "$TPUT" \
                --duration "$DURATION" \
                --throughput-file-prefix "$PREFIX" \
                --num-queries "$NUM_QUERIES"
        fi
    else
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
            --throughput-file-prefix "$PREFIX" \
            --num-queries "$NUM_QUERIES"
    fi

    echo "Waiting 10 seconds to collect idle power usage after run..."
    sleep 10

    stop_powerstat "$MODE"
}

start_cluster() {
    local exec_mode=$1
    echo "[INFO] Configuring and starting Flink cluster for $exec_mode mode..."
    local CONF_FILE="${FLINK_DIR}/conf/flink-conf.yaml"
    local WORKERS_FILE="${FLINK_DIR}/conf/workers"

    # Dynamically configure workers file to isolate execution environments
    if [ -f "$WORKERS_FILE" ]; then
        if [ ! -f "${WORKERS_FILE}.bak" ]; then
            cp "$WORKERS_FILE" "${WORKERS_FILE}.bak"
        fi

        if [ "$exec_mode" = "single" ]; then
            echo "[INFO] Configuring workers file for single node (localhost only)..."
            echo "localhost" > "$WORKERS_FILE"
        else
            echo "[INFO] Configuring workers file for distributed mode (localhost and remote hosts)..."
            echo "localhost" > "$WORKERS_FILE"
            for rhost in "${REMOTE_HOSTS[@]}"; do
                echo "$rhost" >> "$WORKERS_FILE"
            done
        fi
    fi

    if [ -f "$CONF_FILE" ]; then
        local SLOTS=$NUM_QUERIES
        if [ "$exec_mode" = "distributed" ]; then
            SLOTS=$((NUM_QUERIES / 2))
            if [ "$SLOTS" -lt 1 ]; then
                SLOTS=1
            fi
        fi

        if grep -q "^taskmanager.numberOfTaskSlots:" "$CONF_FILE"; then
            sed -i.bak "s/^taskmanager.numberOfTaskSlots:.*/taskmanager.numberOfTaskSlots: $SLOTS/" "$CONF_FILE"
        else
            echo "taskmanager.numberOfTaskSlots: $SLOTS" >> "$CONF_FILE"
        fi

        if grep -q "^taskmanager.memory.process.size:" "$CONF_FILE"; then
            sed -i.bak 's/^taskmanager.memory.process.size:.*/taskmanager.memory.process.size: 16384m/' "$CONF_FILE"
        else
            echo "taskmanager.memory.process.size: 16384m" >> "$CONF_FILE"
        fi

        if grep -q "^taskmanager.memory.network.max:" "$CONF_FILE"; then
            sed -i.bak 's/^taskmanager.memory.network.max:.*/taskmanager.memory.network.max: 2gb/' "$CONF_FILE"
        else
            echo "taskmanager.memory.network.max: 2gb" >> "$CONF_FILE"
        fi

        local PARALLELISM=$NUM_QUERIES

        if grep -q "^parallelism.default:" "$CONF_FILE"; then
            sed -i.bak "s/^parallelism.default:.*/parallelism.default: $PARALLELISM/" "$CONF_FILE"
        else
            echo "parallelism.default: $PARALLELISM" >> "$CONF_FILE"
        fi
    fi
    if [ -f "${FLINK_DIR}/bin/start-cluster.sh" ]; then
        "${FLINK_DIR}/bin/start-cluster.sh"
    fi
}

stop_cluster() {
    echo "[INFO] Shutting down Flink cluster..."
    force_kill_flink
}

for mode in "${EXECUTION_MODES[@]}"; do
    NEEDS_CLUSTER=false
    if [ "$LOCAL" = "false" ]; then
        NEEDS_CLUSTER=true
    fi

    for num_q in "${NUM_QUERIES_LIST[@]}"; do
        NUM_QUERIES=$num_q

        for cache in "${CACHE_SIZES[@]}"; do
            for tput in "${THROUGHPUTS[@]}"; do
                if [ "$NEEDS_CLUSTER" = "true" ]; then
                    start_cluster "$mode"
                    # Wait 5 seconds for TaskManagers to fully boot up and register with JobManager
                    sleep 5
                fi

                run_job "$mode" "$tput" "$INPUT_FILE" "$cache"
                echo "Job finished. Sleeping 5 seconds before cluster shutdown..."
                sleep 5

                if [ "$NEEDS_CLUSTER" = "true" ]; then
                    stop_cluster
                    sleep 2
                fi
            done
        done
    done
done

echo "All experiments completed!"

# Restore workers file if backup exists
if [ -f "${FLINK_DIR}/conf/workers.bak" ]; then
    mv "${FLINK_DIR}/conf/workers.bak" "${FLINK_DIR}/conf/workers"
fi

# Summarize throughput
python3 summarize_throughput.py "$MAIN_DIR"
