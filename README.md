# KV Store Benchmark Tool

A benchmarking tool for various KV stores, supporting workloads similar to YCSB (Yahoo! Cloud Serving Benchmark) and other custom benchmark patterns. Currently supports SurrealKV with an extensible architecture for adding other databases.

## Features

- YCSB standard workloads (A through F)
- CRUD benchmark patterns:
  - Range scans
  - Sequential inserts
  - Random inserts
  - Variable-size key-value operations
  - Mixed operations
- Concurrent client testing
- Detailed latency metrics
- Customizable operation counts and data sizes
- Automatic cleanup of test data
- Support for multiple database types (extensible)

## Command Line Arguments

```
# Command-Line Arguments for Benchmark Tool

## General Arguments
- `--database <DATABASE>`: Specify the database to use (e.g., `surrealkv`, `rocksdb`).
- `--workload <WORKLOAD>`: Specify the workload type (e.g., `a`, `b`, `c`, `d`, `e`, `f`, `range-scan`, `sequential-insert`, `random-insert`, `variable-size`, `mixed-operations`).
- `--benchmark-type <BENCHMARK_TYPE>`: Specify the benchmark type (e.g., `ycsb`, `crud`).

## Workload Configuration
- `--record-count <RECORD_COUNT>`: Number of records to load initially.
- `--operation-count <OPERATION_COUNT>`: Number of operations to perform.
- `--key-size <KEY_SIZE>`: Size of the keys (e.g., `10`, `10:100`).
- `--value-size <VALUE_SIZE>`: Size of the values (e.g., `100`, `100:10000`).
- `--range-size <RANGE_SIZE>`: Size of the range for range scan operations.
- `--load-pattern <LOAD_PATTERN>`: Pattern for loading initial dataset (e.g., `uniform`, `zipfian`).

## Client Configuration
- `--num-clients <NUM_CLIENTS>`: Number of concurrent clients.
- `--worker-threads <WORKER_THREADS>`: Number of worker threads for the Tokio runtime.
- `--thread-stack-size <THREAD_STACK_SIZE>`: Stack size for each thread.
- `--max-blocking-threads <MAX_BLOCKING_THREADS>`: Maximum number of blocking threads.
- `--enable-io`: Enable IO operations.
- `--enable-time`: Enable time operations.

### YCSB Specific Arguments
- `--read-proportion <READ_PROPORTION>`: Proportion of read operations.
- `--update-proportion <UPDATE_PROPORTION>`: Proportion of update operations.
- `--insert-proportion <INSERT_PROPORTION>`: Proportion of insert operations.
- `--delete-proportion <DELETE_PROPORTION>`: Proportion of delete operations.

## Example Usage
```sh
target/release/benchmarks --database surrealkv --workload range-scan --range-size 100 --record-count 100 --operation-count 100 --num-clients 100 --worker-threads 16
```

## Usage Examples

### Basic Usage

```bash
# Simple YCSB workload
cargo run --release -- --database surrealkv --workload a

# Simple CRUD workload
cargo run --release -- --database surrealkv --benchmark-type crud --workload sequential-insert

# With custom operation count
cargo run --release -- \
    --database surrealkv \
    --workload a \
    --operation-count 100000
```

### Concurrent Testing

```bash
# Run with 10 concurrent clients
cargo run --release -- \
    --database surrealkv \
    --workload a \
    --num-clients 10 \
    --record-count 10000 \
    --operation-count 100000
```

### Variable Size Testing

```bash
# Test with variable key and value sizes
cargo run --release -- \
    --database surrealkv \
    --workload variable_size \
    --key-size "10:100" \
    --value-size "1000:10000" \
    --operation-count 50000
```

### Range Scan Testing

```bash
# Test range scans with sequential data loading (default)
cargo run --release -- \
    --database surrealkv \
    --workload range-scan \
    --range-size 100 \
    --record-count 10000

# Test range scans with random data loading
cargo run --release -- \
    --database surrealkv \
    --workload range-scan \
    --range-size 100 \
    --record-count 10000 \
    --load-pattern random
```

## Workload Types

### YCSB Standard Workloads

- **Workload A (Update Heavy)**
  - 50% reads, 50% updates
  ```bash
  cargo run --release -- --database surrealkv --workload a
  ```

- **Workload B (Read Heavy)**
  - 95% reads, 5% updates
  ```bash
  cargo run --release -- --database surrealkv --workload b
  ```

- **Workload C (Read Only)**
  - 100% reads
  ```bash
  cargo run --release -- --database surrealkv --workload c
  ```

- **Workload D (Read Latest)**
  - 95% reads, 5% inserts
  ```bash
  cargo run --release -- --database surrealkv --workload d
  ```

- **Workload E (Scan Short Ranges)**
  - 95% scans, 5% inserts
  ```bash
  cargo run --release -- --database surrealkv --workload e
  ```

- **Workload F (Read-Modify-Write)**
  - 50% reads, 50% read-modify-write
  ```bash
  cargo run --release -- --database surrealkv --workload f
  ```

### CRUD Workload Types

- **Range Scan**
  - Sequential range scans with configurable range sizes
  ```bash
  cargo run --release -- \
      --database surrealkv \
      --workload range-scan \
      --range-size 100
  ```

- **Sequential Insert**
  - Sequential key insertions
  ```bash
  cargo run --release -- \
      --database surrealkv \
      --workload sequential-insert \
      --operation-count 100000
  ```

- **Random Insert**
  - Random key insertions
  ```bash
  cargo run --release -- \
      --database surrealkv \
      --workload random-insert \
      --operation-count 100000
  ```

- **Variable Size**
  - Operations with variable key and value sizes
  ```bash
  cargo run --release -- \
      --database surrealkv \
      --workload variable-size \
      --key-size "10:100" \
      --value-size "1000:10000"
  ```

- **Mixed Operations**
  - Mix of create, read, update, and delete operations
  ```bash
  cargo run --release -- \
      --database surrealkv \
      --workload mixed-operations \
  ```



### Data Loading Patterns

The benchmark tool supports two patterns for loading initial data:

- **Sequential Loading (default)**
  - Records are inserted in sequential order (user0, user1, user2, ...)
  - Provides predictable data layout
  - Useful for baseline performance measurements

- **Random Loading**
  - Records are inserted in random order
  - Tests database performance with non-sequential insertions
  - Useful for simulating real-world data patterns

Example usage with different loading patterns:
```bash
# Sequential loading (default)
cargo run --release -- \
    --database surrealkv \
    --workload a \
    --load-pattern sequential

# Random loading
cargo run --release -- \
    --database surrealkv \
    --workload a \
    --load-pattern random
```

## Output Format

The benchmark tool provides detailed metrics including:
- Operation counts
- Latency statistics (min, max, median, P95, P99)
- Total data processed
- Operation throughput

Example output:
```
Detailed Latency Statistics:
Operation       Count        Min          Max          Median       P95          P99         
---------------------------------------------------------------------------------------
Reads           5098         625.00 ns    49.42 µs     2.33 µs      2.92 µs      3.38 µs     
Updates         4902         13.25 µs     130.62 µs    17.04 µs     22.54 µs     34.00 µs   
```

## Contributing

Contributions are welcome! To add support for a new database:

1. Implement the `Client` trait for your database
2. Add your database to the `Database` enum
3. Add the database match arm in the main benchmark runner

## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.