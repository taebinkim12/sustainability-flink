# Sustainability-Flink: Energy & Power Benchmarking for Apache Flink

A research testbed and benchmarking suite designed to measure, compare, and analyze the **power consumption, energy efficiency, and throughput** of [Apache Flink](https://flink.apache.org/) stream processing pipelines across **single-node** (standalone / local) and **distributed multi-node** cluster deployments.

---

## Table of Contents

- [Overview & Research Motivation](#overview--research-motivation)
- [Repository Structure](#repository-structure)
- [Workloads & Streaming Queries](#workloads--streaming-queries)
- [Prerequisites & System Setup](#prerequisites--system-setup)
- [Automated Cluster & Worker Management](#automated-cluster--worker-management)
- [Experiment Replication Guide](#experiment-replication-guide)
  - [1. Dataset Preparation](#1-dataset-preparation)
  - [2. Quickstart: Local MiniCluster Execution](#2-quickstart-local-minicluster-execution)
  - [3. Full Experiment: Single vs. Distributed Cluster](#3-full-experiment-single-vs-distributed-cluster)
- [Command-Line Parameters](#command-line-parameters)
- [Output Data & Analysis](#output-data--analysis)
- [Further Documentation](#further-documentation)

---

## Overview & Research Motivation

As stream processing systems scale to handle massive event streams in real time, their energy footprint and operational power draw become critical sustainability concerns. This benchmark evaluates:

1. **Single-Node vs. Distributed Power Profiles**: How power draw, CPU utilization, and energy efficiency scale when executing identical streaming query workloads on a single node versus scaling out across multiple distributed worker nodes.
2. **Query Concurrency & Isolation Overhead**: The energy impact of running multiple concurrent query instances with isolated task slots (`slotSharingGroup`) versus shared execution pools.
3. **Controlled Injection Rate Scaling**: Evaluating power scaling as synthetic input throughput varies from low event rates to saturation using microsecond-precision rate limiting.

---

## Repository Structure

```text
.
├── README.md                           # Project overview & experiment replication guide
├── TECHNICAL_OVERVIEW.md               # Deep technical architecture & internal mechanics
├── run_experiment.sh                   # Main experiment runner & cluster orchestrator
├── summarize_throughput.py             # Python aggregator for throughput & steady-state metrics
├── data/
│   └── 2013_Jan_DQ_headerless_sorted.csv # Sample / sorted benchmark input dataset
└── sustainability-flink/               # Maven Java project containing Flink jobs
    ├── pom.xml                         # Build configuration and dependencies (Flink 1.18.0)
    └── src/main/java/com/flink/sustainability/NYT/
        ├── NYTProfitableQuery.java     # DEBS 2015 Taxi Profitability query implementation
        ├── NYTDistanceQuery.java       # Taxi Average Distance window query implementation
        ├── QueryConfig.java            # Shared parameter parsing & environment configuration
        ├── function/                   # Flink source functions, mappers, windows, and joiners
        │   ├── AbstractNYTSourceOperator.java # Base rate-limiting & throughput sampling source
        │   ├── AreaMapper.java         # 150km x 150km NYC coordinate grid mapping
        │   ├── NYTDQSourceOperator.java
        │   ├── NYTDistanceWindowFunction.java
        │   ├── NYTEmptyTaxiFunction.java
        │   ├── NYTEmptyTaxisCounter.java
        │   ├── NYTProfitFunction.java
        │   ├── NYTProfitJoiner.java
        │   └── NYTSourceOperator.java
        └── types/                      # Custom POJO / Tuple event data structures
```

---

## Workloads & Streaming Queries

The benchmark includes streaming pipelines based on the standard **DEBS 2015 Grand Challenge** NYC Taxi dataset:

1. **`NYTProfitableQuery` (Taxi Profitability)**
   - Maps continuous GPS coordinates into a $600 \times 600$ discrete grid (250m cells).
   - Computes total profit (fares + tips) per cell in a 15-minute sliding window (sliding every 10 seconds).
   - Computes empty taxis per cell in a 15-minute sliding window (sliding every 10 seconds).
   - Performs a windowed interval join between profitable areas and empty taxi counts to rank the most profitable pickup areas.

2. **`NYTDistanceQuery` (Taxi Trip Distance)**
   - Filters events for active trips by vendor and minimum distance threshold.
   - Calculates the tumbling window (2-second) average trip distance aggregated by pickup cell.

---

## Prerequisites & System Setup

### Hardware & Operating System
- Linux (Ubuntu 20.04 / 22.04 LTS recommended) on all nodes with Intel/AMD CPUs supporting RAPL energy counters.
- Master (JobManager) node with network reachability to all remote worker (TaskManager) nodes.

### Software Dependencies
- **Java**: OpenJDK 11 or 17.
- **Apache Maven**: Version 3.8+.
- **Apache Flink**: Binary distribution of Flink 1.18.0 located either in `$PATH` or extracted directly at `./flink-1.18.0`.
- **Python**: Python 3.8+ for metrics aggregation.
- **powerstat**: Linux power sampling utility (`sudo apt-get install powerstat`).

### SSH & Permissions Setup
1. **Passwordless SSH**: The master node must be able to SSH passwordlessly into all worker nodes (and localhost):
   ```bash
   ssh-copy-id localhost
   ssh-copy-id user@remote-worker-1
   ssh-copy-id user@remote-worker-2
   ```
2. **Passwordless Sudo for `powerstat`**: `powerstat` requires root privileges to read MSR / RAPL registers without prompting for a password. Add the following line to `/etc/sudoers` (via `sudo visudo`) on **all** nodes:
   ```text
   <username> ALL=(ALL) NOPASSWD: /usr/sbin/powerstat, /usr/bin/powerstat, /bin/kill, /usr/bin/kill, /usr/bin/pkill
   ```

---

## Automated Cluster & Worker Management

> [!TIP]
> **No manual editing of `${FLINK_DIR}/conf/workers` is required.**
> The experiment runner script (`run_experiment.sh`) automatically configures and restores the Flink workers list dynamically.

### How it works:
- When running in **Single-Node Mode** (`--execution-mode single`), the script automatically restricts `${FLINK_DIR}/conf/workers` to `localhost`, ensuring no tasks or powerstat processes are dispatched to remote machines.
- When running in **Distributed Mode** (`--execution-mode distributed`), simply supply your remote worker hostnames or IPs via the `--remote-hosts` argument:
  ```bash
  ./run_experiment.sh --execution-mode distributed --remote-hosts worker-node-1 worker-node-2
  ```
  The script will automatically write `localhost` plus all specified remote hosts into `${FLINK_DIR}/conf/workers`, launch the distributed TaskManagers via Flink's `start-cluster.sh`, start synchronized remote `powerstat` instances over SSH, and restore the original `workers` file when finished.

---

## Experiment Replication Guide

### 1. Dataset Preparation
Ensure the sorted New York City Taxi CSV dataset is available. By default, the script looks for:
```bash
$HOME/NYT-data/2013_header_less_sorted.csv
```
You can point to an alternative path (e.g., the sample dataset included in `data/`) using `--input-file`:
```bash
--input-file "$PWD/data/2013_Jan_DQ_headerless_sorted.csv"
```

### 2. Quickstart: Local MiniCluster Execution
To test the pipeline locally using Flink's embedded MiniCluster (ideal for verification without starting a standalone cluster):
```bash
./run_experiment.sh \
  --local true \
  --query profitable \
  --num-queries 2 \
  --duration 60 \
  --throughput 0.01 \
  --input-file "$PWD/data/2013_Jan_DQ_headerless_sorted.csv"
```

### 3. Full Experiment: Single vs. Distributed Cluster
To execute an automated multi-configuration experiment comparing single-node and distributed deployments:

```bash
./run_experiment.sh \
  --local false \
  --query profitable \
  --execution-mode single distributed \
  --remote-hosts worker-node-1 worker-node-2 \
  --num-queries 2 4 \
  --throughput 0.01 0.05 0.1 \
  --cache-size 100000 \
  --duration 300 \
  --input-file "$HOME/NYT-data/2013_header_less_sorted.csv"
```

This will automatically:
1. Compile the project with Maven and export the runtime classpath.
2. For each execution mode (`single`, `distributed`), configure `conf/workers` and `flink-conf.yaml`.
3. Launch the Flink cluster and synchronized `powerstat` instances on local and remote hosts.
4. Run the workload for 300 seconds (with 10-second idle power buffers before and after).
5. Stop `powerstat`, cleanly shut down the cluster, and record throughput metrics.
6. Run `summarize_throughput.py` to produce a comprehensive summary.

---

## Command-Line Parameters

| Parameter | Options / Format | Default | Description |
| :--- | :--- | :--- | :--- |
| `--query` | `profitable`, `distance` | `profitable` | Streaming query workload to execute. |
| `--local` | `true`, `false` | Auto (`false` if distributed mode present) | If `true`, runs locally via embedded MiniCluster. If `false`, launches a standalone Flink cluster. |
| `--execution-mode` | `single`, `distributed` (list) | `single` | Cluster execution mode(s) to iterate over. |
| `--remote-hosts` | List of hostnames / IPs | `()` | Hostnames or IPs of remote worker nodes (required for distributed mode). |
| `--num-queries` | Space-separated integers (e.g. `1 2 4`) | `4` | Number of concurrent query instances to run in the job. |
| `--isolate-queries`| `true`, `false` | `true` | When `true`, assigns each query to an isolated slot sharing group (`query_group_i`). |
| `--throughput` | Space-separated floats (e.g. `0.01 0.05`) | `0.01` | Target event rate per query in **millions of events/sec** (e.g. `0.01` = 10,000 eps). |
| `--cache-size` | Space-separated integers | `100000` | Number of events pre-loaded into memory per subtask to avoid disk I/O bottlenecks. |
| `--duration` | Seconds (integer) | `600` | Active execution duration for each run in seconds. |
| `--input-file` | File path | `~/NYT-data/2013_header_less_sorted.csv` | Path to the sorted input CSV dataset. |
| `--taskmanager-slots` | Integer | Calculated dynamically | Override for `taskmanager.numberOfTaskSlots`. |
| `--taskmanager-memory`| Memory string (e.g. `16384m`) | `16384m` | Override for `taskmanager.memory.process.size`. |

---

## Output Data & Analysis

Each execution batch creates a timestamped folder: `YYYYMMDD_HHMM/YYYYMMDD_HHMMSS/` containing:

- **`config.txt`**: Execution metadata, timestamps for start/end of each query subtask, and configuration parameters.
- **`powerstat_output_<hostname>.txt`**: Raw power measurements (Watts, CPU frequency, C-states, RAPL domain energy) sampled at 1 Hz from the local host and all remote workers.
- **`throughput_results_q<Q>_subtask_<S>.csv`**: Per-subtask throughput samples, total events emitted, and steady-state average throughput.
- **`result.txt`**: Summarized total processed throughput across all queries for that run.

To re-run metric summarization across an experiment batch directory:
```bash
python3 summarize_throughput.py <experiment_batch_dir>
```

