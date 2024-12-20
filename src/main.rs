mod args;
mod client;
mod database;
mod metrics;
mod rocksdb;
mod surrealkv;
mod workload;

use metrics::ConcurrentMetrics;
use rocksdb::RocksDBClient;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task;
use workload::{
    generate_random_key, generate_random_value, KeySizeConfig, SizeDistribution, ValueSizeConfig,
    WorkloadConfig,
};

use anyhow::Result;
use args::BenchmarkType;
use clap::Parser;
use surrealkv::SurrealKVClient;

use crate::args::Args;
use crate::client::Client;
use crate::database::Database;
use crate::metrics::Metrics;
use workload::{Workload, WorkloadType};

fn main() {
    let args = Args::parse();
    let runtime = configure_runtime(&args);
    let config = create_workload_config(&args);

    runtime.block_on(async {
        match args.benchmark_type {
            BenchmarkType::Ycsb => run_bench(&args, config).await,
            _ => panic!("Unsupported benchmark type"),
        }
    });
}

fn configure_runtime(args: &Args) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(args.worker_threads as usize);

    if let Some(stack_size) = args.thread_stack_size {
        builder.thread_stack_size(stack_size);
    }

    if let Some(max_threads) = args.max_blocking_threads {
        builder.max_blocking_threads(max_threads);
    }

    match (args.enable_io, args.enable_time) {
        (Some(true), Some(true)) => builder.enable_all(),
        (Some(true), _) => builder.enable_io(),
        (_, Some(true)) => builder.enable_time(),
        _ => &mut builder,
    };

    builder.build().unwrap()
}

fn create_workload_config(args: &Args) -> WorkloadConfig {
    let key_size = args
        .key_size
        .as_ref()
        .map(|s| parse_size_config(s).unwrap());
    let value_size = args
        .value_size
        .as_ref()
        .map(|s| parse_value_size_config(s).unwrap());

    WorkloadConfig {
        record_count: args.record_count,
        operation_count: args.operation_count,
        key_size,
        value_size,
        range_size: args.range_size,
        load_pattern: args.load_pattern,
    }
}

async fn run_bench(args: &Args, config: WorkloadConfig) {
    if let Some(workload) = create_workload(args.workload.clone().unwrap(), &args) {
        match args.database {
            #[cfg(feature = "surrealkv")]
            Database::Surrealkv => {
                let client = SurrealKVClient::new().unwrap();
                run_concurrent_benchmark(client, workload, args.num_clients, args.load_pattern)
                    .await
                    .unwrap();
            }
            #[cfg(feature = "rocksdb")]
            Database::Rocksdb => {
                let client = RocksDBClient::new().unwrap();
                run_concurrent_benchmark(client, workload, args.num_clients, args.load_pattern)
                    .await
                    .unwrap();
            }
        }
    } else {
        match args.database {
            #[cfg(feature = "surrealkv")]
            Database::Surrealkv => {
                let client = SurrealKVClient::new().unwrap();
                execute_workload(client, args.workload.clone().unwrap(), config)
                    .await
                    .unwrap();
            }
            #[cfg(feature = "rocksdb")]
            Database::Rocksdb => {
                let client = RocksDBClient::new().unwrap();
                execute_workload(client, args.workload.clone().unwrap(), config)
                    .await
                    .unwrap();
            }
        }
    }
}

fn create_workload(workload_type: WorkloadType, args: &Args) -> Option<Workload> {
    match workload_type {
        WorkloadType::A => Some(Workload::WorkloadA {
            read_proportion: args.read_proportion,
            record_count: args.record_count,
            operation_count: args.operation_count,
        }),
        WorkloadType::B => Some(Workload::WorkloadB {
            read_proportion: 0.95,
            record_count: args.record_count,
            operation_count: args.operation_count,
        }),
        WorkloadType::C => Some(Workload::WorkloadC {
            read_proportion: 1.0,
            record_count: args.record_count,
            operation_count: args.operation_count,
        }),
        WorkloadType::D => Some(Workload::WorkloadD {
            read_proportion: 0.95,
            record_count: args.record_count,
            operation_count: args.operation_count,
        }),
        WorkloadType::E => Some(Workload::WorkloadE {
            scan_proportion: 0.95,
            record_count: args.record_count,
            operation_count: args.operation_count,
        }),
        WorkloadType::F => Some(Workload::WorkloadF {
            read_modify_write_proportion: 0.5,
            record_count: args.record_count,
            operation_count: args.operation_count,
        }),
        _ => None,
    }
}

async fn execute_workload<C: Client + Clone + 'static>(
    client: C,
    workload_type: WorkloadType,
    config: WorkloadConfig,
) -> Result<()> {
    match workload_type {
        WorkloadType::RangeScan => {
            let metrics = run_range_scan_benchmark(client, config).await?;
            println!("Range Scan Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::SequentialInsert => {
            let metrics = run_sequential_insert_benchmark(client, config).await?;
            println!("Sequential Insert Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::RandomInsert => {
            let metrics = run_random_insert_benchmark(client, config).await?;
            println!("Random Insert Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::VariableSize => {
            let metrics = run_variable_size_benchmark(client, config).await?;
            println!("Variable Size Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::MixedOperations => {
            let metrics = run_mixed_operations_benchmark(client, config).await?;
            println!("Mixed Operations Benchmark Results:");
            println!("{}", metrics);
        }
        _ => panic!("Unsupported workload type for execution"),
    }
    Ok(())
}

fn generate_value() -> serde_json::Value {
    serde_json::json!({
        "field1": rand::random::<u32>(),
        "field2": format!("value{}", rand::random::<u32>()),
        "field3": vec![1, 2, 3, 4, 5],
        "timestamp": chrono::Utc::now().timestamp(),
        "data": vec![0u8; 100],
    })
}

async fn run_concurrent_benchmark<C: Client + Clone + 'static>(
    client: C,
    workload: Workload,
    num_clients: u32,
    load_pattern: workload::LoadPattern,
) -> Result<()> {
    println!(
        "Starting benchmark with {} concurrent clients...",
        num_clients
    );

    let metrics = ConcurrentMetrics::default();
    let total_bytes_read = Arc::new(AtomicUsize::new(0));
    let total_operations = Arc::new(AtomicU32::new(0));

    match &workload {
        Workload::WorkloadA {
            read_proportion,
            record_count,
            operation_count,
        } => {
            println!("Running Workload A (50% reads, 50% updates)");
            println!("Loading initial dataset with {:?} pattern...", load_pattern);
            client
                .load_initial_dataset(*record_count, load_pattern)
                .await?;

            let verify_key = format!("user{}", rand::random::<u32>() % record_count);
            let verify_read = client.read(&verify_key).await?;
            assert!(
                verify_read.is_some(),
                "Initial data load failed - could not read test key"
            );

            println!("Initial dataset loaded and verified");

            let ops_per_client = operation_count / num_clients;
            println!("Each client will perform {} operations", ops_per_client);

            let mut handles = Vec::new();

            for client_id in 0..num_clients {
                let client = client.clone();
                let metrics = metrics.clone();
                let total_bytes_read = total_bytes_read.clone();
                let total_operations = total_operations.clone();
                let read_prop = *read_proportion;
                let record_count = *record_count;

                let handle = task::spawn(async move {
                    println!("Starting client {}", client_id);

                    for i in 0..ops_per_client {
                        if i % 1000 == 0 {
                            println!(
                                "Client {} progress: {}/{} ({:.1}%)",
                                client_id,
                                i,
                                ops_per_client,
                                (i as f64 / ops_per_client as f64) * 100.0
                            );
                        }

                        if rand::random::<f32>() < read_prop {
                            let key = format!("user{}", rand::random::<u32>() % record_count);
                            let start = std::time::Instant::now();
                            let result = client.read(&key).await?;
                            metrics.record_read(start.elapsed()).await;

                            if let Some(value) = result {
                                let bytes = serde_json::to_string(&value)?.len();
                                total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
                            }
                        } else {
                            let key = format!("user{}", rand::random::<u32>() % record_count);
                            let value = generate_value();
                            let start = std::time::Instant::now();
                            client.update(&key, value).await?;
                            metrics.record_update(start.elapsed()).await;
                        }

                        total_operations.fetch_add(1, Ordering::Relaxed);
                    }

                    Ok::<_, anyhow::Error>(())
                });

                handles.push(handle);
            }

            for handle in handles {
                handle.await??;
            }
        }
        _ => println!("Workload not implemented yet"),
    }

    println!("\nBenchmark Complete");
    println!(
        "Total Operations: {}",
        total_operations.load(Ordering::Relaxed)
    );
    println!(
        "Total Data Read: {:.2} MB",
        total_bytes_read.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    println!("{}", metrics.get_metrics().await);

    Ok(())
}

pub async fn run_range_scan_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();

    println!("Running Range Scan Benchmark");
    println!(
        "Loading initial dataset with {:?} pattern...",
        config.load_pattern
    );

    client
        .load_initial_dataset(config.record_count, config.load_pattern)
        .await?;

    let range_size = config.range_size.unwrap_or(100);
    println!("Running range scans with size: {}", range_size);

    let mut current_index = 0;

    for i in 0..config.operation_count {
        if i % 100 == 0 {
            println!("Progress: {}/{}", i, config.operation_count);
        }

        let start_key = format!("user{}", current_index);

        let start = std::time::Instant::now();
        let results = client.scan(&start_key, Some(range_size as usize)).await?;
        metrics.record_scan(start.elapsed()).await;

        assert!(!results.is_empty(), "Range scan returned no results");

        current_index = (current_index + range_size) % config.record_count;
    }

    Ok(metrics.get_metrics().await)
}

pub async fn run_variable_size_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();

    let key_config = config.key_size.unwrap_or(KeySizeConfig {
        min_size: 10,
        max_size: 100,
        distribution: SizeDistribution::Uniform,
    });

    let value_config = config.value_size.unwrap_or(ValueSizeConfig {
        min_size: 100,
        max_size: 10000,
        distribution: SizeDistribution::Uniform,
    });

    for i in 0..config.operation_count {
        if i % 100 == 0 {
            println!("Progress: {}/{}", i, config.operation_count);
        }

        let key = generate_random_key(&key_config);
        let value = generate_random_value(&value_config);

        let start = std::time::Instant::now();
        client.insert(&key, value).await?;
        metrics.record_insert(start.elapsed()).await;
    }

    Ok(metrics.get_metrics().await)
}

fn parse_size_config(size_str: &str) -> Result<KeySizeConfig> {
    if let Some((min, max)) = size_str.split_once(':') {
        Ok(KeySizeConfig {
            min_size: min.parse()?,
            max_size: max.parse()?,
            distribution: SizeDistribution::Uniform,
        })
    } else {
        let size = size_str.parse()?;
        Ok(KeySizeConfig {
            min_size: size,
            max_size: size,
            distribution: SizeDistribution::Fixed(size),
        })
    }
}

fn parse_value_size_config(size_str: &str) -> Result<ValueSizeConfig> {
    if let Some((min, max)) = size_str.split_once(':') {
        Ok(ValueSizeConfig {
            min_size: min.parse()?,
            max_size: max.parse()?,
            distribution: SizeDistribution::Uniform,
        })
    } else {
        let size = size_str.parse()?;
        Ok(ValueSizeConfig {
            min_size: size,
            max_size: size,
            distribution: SizeDistribution::Fixed(size),
        })
    }
}

async fn run_sequential_insert_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    println!("Running Sequential Insert Benchmark");

    for i in 0..config.operation_count {
        if i % 1000 == 0 {
            println!("Progress: {}/{}", i, config.operation_count);
        }

        let key = format!("sequential_key_{}", i);
        let value = generate_value();

        let start = std::time::Instant::now();
        client.insert(&key, value).await?;
        metrics.record_insert(start.elapsed()).await;
    }

    Ok(metrics.get_metrics().await)
}

async fn run_random_insert_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    println!("Running Random Insert Benchmark");

    for i in 0..config.operation_count {
        if i % 1000 == 0 {
            println!("Progress: {}/{}", i, config.operation_count);
        }

        let random_key = format!("random_key_{}", rand::random::<u32>());
        let value = generate_value();

        let start = std::time::Instant::now();
        client.insert(&random_key, value).await?;
        metrics.record_insert(start.elapsed()).await;
    }

    Ok(metrics.get_metrics().await)
}

async fn run_mixed_operations_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    println!("Running Mixed Operations Benchmark");

    println!(
        "Loading initial dataset with {:?} pattern...",
        config.load_pattern
    );

    client
        .load_initial_dataset(config.record_count, config.load_pattern)
        .await?;

    for i in 0..config.operation_count {
        if i % 1000 == 0 {
            println!("Progress: {}/{}", i, config.operation_count);
        }

        match rand::random::<u32>() % 4 {
            0 => {
                let key = format!("mixed_key_{}", rand::random::<u32>());
                let value = generate_value();
                let start = std::time::Instant::now();
                client.insert(&key, value).await?;
                metrics.record_insert(start.elapsed()).await;
            }
            1 => {
                let key = format!("mixed_key_{}", rand::random::<u32>() % i);
                let start = std::time::Instant::now();
                client.read(&key).await?;
                metrics.record_read(start.elapsed()).await;
            }
            2 => {
                let key = format!("mixed_key_{}", rand::random::<u32>() % i);
                let value = generate_value();
                let start = std::time::Instant::now();
                client.update(&key, value).await?;
                metrics.record_update(start.elapsed()).await;
            }
            3 => {
                let key = format!("mixed_key_{}", rand::random::<u32>() % i);
                let start = std::time::Instant::now();
                client.delete(&key).await?;
                metrics.record_delete(start.elapsed()).await;
            }
            _ => unreachable!(),
        }
    }

    Ok(metrics.get_metrics().await)
}
