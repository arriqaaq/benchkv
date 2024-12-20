# KV Store Benchmark Tool

A benchmarking tool for various KV stores, supporting workloads similar to YCSB (Yahoo! Cloud Serving Benchmark) and other custom benchmark patterns. Currently supports SurrealKV with an extensible architecture for adding other databases.

## Features

- YCSB standard workloads (A through F)
- Custom benchmark patterns:
  - Range scans
  - Sequential inserts
  - Random inserts
  - Variable-size key-value operations
  - Mixed operations (CRUD)
- Concurrent client testing
- Detailed latency metrics
- Customizable operation counts and data sizes
- Automatic cleanup of test data
- Support for multiple database types (extensible)

## Command Line Arguments

```
REQUIRED:
--database <DATABASE>        Database to benchmark (e.g., surrealkv)
--workload <TYPE>           Workload type to run

OPTIONAL:
--num-clients <NUM>         Number of concurrent clients [default: 1]
--record-count <NUM>        Number of initial records [default: 1000]
--operation-count <NUM>     Number of operations to perform [default: 10000]
--read-proportion <NUM>     Proportion of read operations [default: 0.5]
--key-size <SIZE>          Key size config ("min:max" or fixed size)
--value-size <SIZE>        Value size config ("min:max" or fixed size)
--range-size <SIZE>        Range size for scan operations
--load-pattern <PATTERN>    Pattern for loading initial data (sequential/random) [default: sequential]
```


## Workload Types

### YCSB Standard Workloads

- **Workload A (Update Heavy)**
  - 50% reads, 50% updates
  - Example: Session store recording recent actions
  ```bash
  cargo run --release -- --database surrealkv --workload a
  ```

- **Workload B (Read Heavy)**
  - 95% reads, 5% updates
  - Example: Photo tagging
  ```bash
  cargo run --release -- --database surrealkv --workload b
  ```

- **Workload C (Read Only)**
  - 100% reads
  - Example: User profile cache
  ```bash
  cargo run --release -- --database surrealkv --workload c
  ```

- **Workload D (Read Latest)**
  - 95% reads, 5% inserts
  - Example: User status updates
  ```bash
  cargo run --release -- --database surrealkv --workload d
  ```

- **Workload E (Scan Short Ranges)**
  - 95% scans, 5% inserts
  - Example: Threaded conversations
  ```bash
  cargo run --release -- --database surrealkv --workload e
  ```

- **Workload F (Read-Modify-Write)**
  - 50% reads, 50% read-modify-write
  - Example: User database
  ```bash
  cargo run --release -- --database surrealkv --workload f
  ```

### Custom Workload Types

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

## Command Line Arguments

```
REQUIRED:
--database <DATABASE>        Database to benchmark (e.g., surrealkv)
--workload <TYPE>      Workload type to run

OPTIONAL:
--num-clients <NUM>         Number of concurrent clients [default: 1]
--record-count <NUM>        Number of initial records [default: 1000]
--operation-count <NUM>     Number of operations to perform [default: 10000]
--read-proportion <NUM>     Proportion of read operations [default: 0.5]
--key-size <SIZE>          Key size config ("min:max" or fixed size)
--value-size <SIZE>        Value size config ("min:max" or fixed size)
--range-size <SIZE>        Range size for scan operations
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