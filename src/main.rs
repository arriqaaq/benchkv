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
            BenchmarkType::Ycsb => run_ycsb(&args).await,
            BenchmarkType::Crud => run_crud(&args, config).await,
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

async fn run_ycsb(args: &Args) {
    if let Some(workload) = create_workload(args.workload.clone().unwrap(), args) {
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
    }
}

async fn run_crud(args: &Args, config: WorkloadConfig) {
    match args.database {
        #[cfg(feature = "surrealkv")]
        Database::Surrealkv => {
            let client = SurrealKVClient::new().unwrap();
            run_crud_benchmark(
                client,
                args.workload.clone().unwrap(),
                config,
                args.num_clients,
            )
            .await
            .unwrap();
        }
        #[cfg(feature = "rocksdb")]
        Database::Rocksdb => {
            let client = RocksDBClient::new().unwrap();
            run_crud_benchmark(
                client,
                args.workload.clone().unwrap(),
                config,
                args.num_clients,
            )
            .await
            .unwrap();
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

async fn run_crud_benchmark<C: Client + Clone + 'static>(
    client: C,
    workload_type: WorkloadType,
    config: WorkloadConfig,
    num_clients: u32,
) -> Result<()> {
    match workload_type {
        WorkloadType::RangeScan => {
            let metrics = run_range_scan_benchmark(client, config, num_clients).await?;
            println!("Range Scan Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::SequentialInsert => {
            let metrics = run_sequential_insert_benchmark(client, config, num_clients).await?;
            println!("Sequential Insert Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::RandomInsert => {
            let metrics = run_random_insert_benchmark(client, config, num_clients).await?;
            println!("Random Insert Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::VariableSize => {
            let metrics = run_variable_size_benchmark(client, config, num_clients).await?;
            println!("Variable Size Benchmark Results:");
            println!("{}", metrics);
        }
        WorkloadType::MixedOperations => {
            let metrics = run_mixed_operations_benchmark(client, config, num_clients).await?;
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

pub async fn run_range_scan_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
    num_clients: u32,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    let mut handles = Vec::new();

    println!(
        "Running Range Scan Benchmark with {} concurrent clients",
        num_clients
    );
    println!(
        "Loading initial dataset with {:?} pattern...",
        config.load_pattern
    );

    client
        .load_initial_dataset(config.record_count, config.load_pattern)
        .await?;

    let range_size = config.range_size.unwrap_or(100);
    println!("Running range scans with size: {}", range_size);

    for client_id in 0..num_clients {
        let client = client.clone();
        let metrics = metrics.clone();
        let config = config.clone();

        let handle = task::spawn(async move {
            let mut current_index = 0;

            for i in 0..config.operation_count / num_clients {
                if i % 100 == 0 {
                    println!(
                        "Client {} progress: {}/{}",
                        client_id,
                        i,
                        config.operation_count / num_clients
                    );
                }

                let start_key = format!("user{}", current_index);

                let start = std::time::Instant::now();
                let results = client.scan(&start_key, Some(range_size as usize)).await?;
                metrics.record_scan(start.elapsed()).await;

                assert!(!results.is_empty(), "Range scan returned no results");

                current_index = (current_index + range_size) % config.record_count;
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(metrics.get_metrics().await)
}

pub async fn run_variable_size_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
    num_clients: u32,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    let mut handles = Vec::new();

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

    println!(
        "Running Variable Size Benchmark with {} concurrent clients",
        num_clients
    );

    for client_id in 0..num_clients {
        let client = client.clone();
        let metrics = metrics.clone();
        let key_config = key_config.clone();
        let value_config = value_config.clone();

        let handle = task::spawn(async move {
            for i in 0..config.operation_count / num_clients {
                if i % 100 == 0 {
                    println!(
                        "Client {} progress: {}/{}",
                        client_id,
                        i,
                        config.operation_count / num_clients
                    );
                }

                let key = generate_random_key(&key_config);
                let value = generate_random_value(&value_config);

                let start = std::time::Instant::now();
                client.insert(&key, value).await?;
                metrics.record_insert(start.elapsed()).await;
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(metrics.get_metrics().await)
}

async fn run_sequential_insert_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
    num_clients: u32,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    let mut handles = Vec::new();

    println!(
        "Running Sequential Insert Benchmark with {} concurrent clients",
        num_clients
    );

    for client_id in 0..num_clients {
        let client = client.clone();
        let metrics = metrics.clone();
        let config = config.clone();

        let handle = task::spawn(async move {
            for i in 0..config.operation_count / num_clients {
                if i % 1000 == 0 {
                    println!(
                        "Client {} progress: {}/{}",
                        client_id,
                        i,
                        config.operation_count / num_clients
                    );
                }

                let key = format!(
                    "sequential_key_{}",
                    i + client_id * (config.operation_count / num_clients)
                );
                let value = generate_value();

                let start = std::time::Instant::now();
                client.insert(&key, value).await?;
                metrics.record_insert(start.elapsed()).await;
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(metrics.get_metrics().await)
}

async fn run_random_insert_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
    num_clients: u32,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    let mut handles = Vec::new();

    println!(
        "Running Random Insert Benchmark with {} concurrent clients",
        num_clients
    );

    for client_id in 0..num_clients {
        let client = client.clone();
        let metrics = metrics.clone();
        let config = config.clone();

        let handle = task::spawn(async move {
            for i in 0..config.operation_count / num_clients {
                if i % 1000 == 0 {
                    println!(
                        "Client {} progress: {}/{}",
                        client_id,
                        i,
                        config.operation_count / num_clients
                    );
                }

                let random_key = format!("random_key_{}", rand::random::<u32>());
                let value = generate_value();

                let start = std::time::Instant::now();
                client.insert(&random_key, value).await?;
                metrics.record_insert(start.elapsed()).await;
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(metrics.get_metrics().await)
}

async fn run_mixed_operations_benchmark<C: Client + Clone + 'static>(
    client: C,
    config: WorkloadConfig,
    num_clients: u32,
) -> Result<Metrics> {
    let metrics = ConcurrentMetrics::default();
    let mut handles = Vec::new();

    println!(
        "Running Mixed Operations Benchmark with {} concurrent clients",
        num_clients
    );
    println!(
        "Loading initial dataset with {:?} pattern...",
        config.load_pattern
    );

    client
        .load_initial_dataset(config.record_count, config.load_pattern)
        .await?;

    for client_id in 0..num_clients {
        let client = client.clone();
        let metrics = metrics.clone();
        let config = config.clone();

        let handle = task::spawn(async move {
            for i in 0..config.operation_count / num_clients {
                if i % 1000 == 0 {
                    println!(
                        "Client {} progress: {}/{}",
                        client_id,
                        i,
                        config.operation_count / num_clients
                    );
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
                        let key =
                            format!("mixed_key_{}", rand::random::<u32>() % config.record_count);
                        let start = std::time::Instant::now();
                        client.read(&key).await?;
                        metrics.record_read(start.elapsed()).await;
                    }
                    2 => {
                        let key =
                            format!("mixed_key_{}", rand::random::<u32>() % config.record_count);
                        let value = generate_value();
                        let start = std::time::Instant::now();
                        client.update(&key, value).await?;
                        metrics.record_update(start.elapsed()).await;
                    }
                    3 => {
                        let key =
                            format!("mixed_key_{}", rand::random::<u32>() % config.record_count);
                        let start = std::time::Instant::now();
                        client.delete(&key).await?;
                        metrics.record_delete(start.elapsed()).await;
                    }
                    _ => unreachable!(),
                }
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(metrics.get_metrics().await)
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

    // Load initial dataset based on workload type
    let record_count = workload.get_record_count();
    println!("Loading initial dataset with {:?} pattern...", load_pattern);
    client
        .load_initial_dataset(record_count, load_pattern)
        .await?;

    // Verify initial data load
    let verify_key = format!("user{}", rand::random::<u32>() % record_count);
    let verify_read = client.read(&verify_key).await?;
    assert!(
        verify_read.is_some(),
        "Initial data load failed - could not read test key"
    );
    println!("Initial dataset loaded and verified");

    // Calculate operations per client
    let operation_count = workload.get_operation_count();
    let ops_per_client = operation_count / num_clients;
    println!("Each client will perform {} operations", ops_per_client);

    // Create client tasks
    let mut handles = Vec::new();
    for client_id in 0..num_clients {
        let client = client.clone();
        let metrics = metrics.clone();
        let total_bytes_read = total_bytes_read.clone();
        let total_operations = total_operations.clone();
        let workload = workload.clone();

        let handle = task::spawn(async move {
            match &workload {
                Workload::WorkloadA {
                    read_proportion, ..
                } => {
                    run_workload_a(
                        client_id,
                        client,
                        *read_proportion,
                        record_count,
                        ops_per_client,
                        &metrics,
                        &total_bytes_read,
                        &total_operations,
                    )
                    .await
                }
                Workload::WorkloadB {
                    read_proportion, ..
                } => {
                    run_workload_b(
                        client_id,
                        client,
                        *read_proportion,
                        record_count,
                        ops_per_client,
                        &metrics,
                        &total_bytes_read,
                        &total_operations,
                    )
                    .await
                }
                Workload::WorkloadC { .. } => {
                    run_workload_c(
                        client_id,
                        client,
                        record_count,
                        ops_per_client,
                        &metrics,
                        &total_bytes_read,
                        &total_operations,
                    )
                    .await
                }
                Workload::WorkloadD {
                    read_proportion, ..
                } => {
                    run_workload_d(
                        client_id,
                        client,
                        *read_proportion,
                        record_count,
                        ops_per_client,
                        &metrics,
                        &total_bytes_read,
                        &total_operations,
                    )
                    .await
                }
                Workload::WorkloadE {
                    scan_proportion, ..
                } => {
                    run_workload_e(
                        client_id,
                        client,
                        *scan_proportion,
                        record_count,
                        ops_per_client,
                        &metrics,
                        &total_operations,
                    )
                    .await
                }
                Workload::WorkloadF {
                    read_modify_write_proportion,
                    ..
                } => {
                    run_workload_f(
                        client_id,
                        client,
                        *read_modify_write_proportion,
                        record_count,
                        ops_per_client,
                        &metrics,
                        &total_bytes_read,
                        &total_operations,
                    )
                    .await
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all clients to complete
    for handle in handles {
        handle.await??;
    }

    // Print results
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

// Individual workload implementations
async fn run_workload_a<C: Client>(
    client_id: u32,
    client: C,
    read_proportion: f32,
    record_count: u32,
    ops_per_client: u32,
    metrics: &ConcurrentMetrics,
    total_bytes_read: &AtomicUsize,
    total_operations: &AtomicU32,
) -> Result<()> {
    println!("Starting client {} for Workload A", client_id);

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

        if rand::random::<f32>() < read_proportion {
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

    Ok(())
}

async fn run_workload_e<C: Client>(
    client_id: u32,
    client: C,
    scan_proportion: f32,
    record_count: u32,
    ops_per_client: u32,
    metrics: &ConcurrentMetrics,
    total_operations: &AtomicU32,
) -> Result<()> {
    println!("Starting client {} for Workload E (Scan)", client_id);

    // Use a default range size or get it from configuration
    let range_size = 100; // Default range size for scans

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

        if rand::random::<f32>() < scan_proportion {
            let start_key = format!("user{}", rand::random::<u32>() % record_count);
            let start = std::time::Instant::now();
            let results = client.scan(&start_key, Some(range_size as usize)).await?;
            metrics.record_scan(start.elapsed()).await;
            assert!(!results.is_empty(), "Scan returned no results");
        } else {
            let key = format!("user{}", record_count + i);
            let value = generate_value();
            let start = std::time::Instant::now();
            client.insert(&key, value).await?;
            metrics.record_insert(start.elapsed()).await;
        }

        total_operations.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

// Workload B: Read heavy workload (95% reads, 5% updates)
async fn run_workload_b<C: Client>(
    client_id: u32,
    client: C,
    read_proportion: f32,
    record_count: u32,
    ops_per_client: u32,
    metrics: &ConcurrentMetrics,
    total_bytes_read: &AtomicUsize,
    total_operations: &AtomicU32,
) -> Result<()> {
    println!("Starting client {} for Workload B (Read Heavy)", client_id);

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

        if rand::random::<f32>() < read_proportion {
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

    Ok(())
}

// Workload C: Read only workload (100% reads)
async fn run_workload_c<C: Client>(
    client_id: u32,
    client: C,
    record_count: u32,
    ops_per_client: u32,
    metrics: &ConcurrentMetrics,
    total_bytes_read: &AtomicUsize,
    total_operations: &AtomicU32,
) -> Result<()> {
    println!("Starting client {} for Workload C (Read Only)", client_id);

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

        let key = format!("user{}", rand::random::<u32>() % record_count);
        let start = std::time::Instant::now();
        let result = client.read(&key).await?;
        metrics.record_read(start.elapsed()).await;

        if let Some(value) = result {
            let bytes = serde_json::to_string(&value)?.len();
            total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        }

        total_operations.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

// Workload D: Read latest workload (95% reads, 5% inserts)
async fn run_workload_d<C: Client>(
    client_id: u32,
    client: C,
    read_proportion: f32,
    record_count: u32,
    ops_per_client: u32,
    metrics: &ConcurrentMetrics,
    total_bytes_read: &AtomicUsize,
    total_operations: &AtomicU32,
) -> Result<()> {
    println!("Starting client {} for Workload D (Read Latest)", client_id);

    // Keep track of new insertions
    let new_records = Arc::new(AtomicU32::new(0));

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

        if rand::random::<f32>() < read_proportion {
            // Read operation - bias towards recently inserted records
            let total_records = record_count + new_records.load(Ordering::Relaxed);
            let recent_window = std::cmp::min(100, total_records); // Read from latest 100 records
            let offset = rand::random::<u32>() % recent_window;
            let key = format!("user{}", total_records - offset - 1);

            let start = std::time::Instant::now();
            let result = client.read(&key).await?;
            metrics.record_read(start.elapsed()).await;

            if let Some(value) = result {
                let bytes = serde_json::to_string(&value)?.len();
                total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
            }
        } else {
            // Insert operation - always add new records
            let new_record_num = new_records.fetch_add(1, Ordering::Relaxed);
            let key = format!("user{}", record_count + new_record_num);
            let value = generate_value();

            let start = std::time::Instant::now();
            client.insert(&key, value).await?;
            metrics.record_insert(start.elapsed()).await;
        }

        total_operations.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

// Workload F: Read-modify-write (50% reads, 50% read-modify-write)
async fn run_workload_f<C: Client>(
    client_id: u32,
    client: C,
    read_modify_write_proportion: f32,
    record_count: u32,
    ops_per_client: u32,
    metrics: &ConcurrentMetrics,
    total_bytes_read: &AtomicUsize,
    total_operations: &AtomicU32,
) -> Result<()> {
    println!(
        "Starting client {} for Workload F (Read-Modify-Write)",
        client_id
    );

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

        if rand::random::<f32>() < read_modify_write_proportion {
            // Read-modify-write operation
            let key = format!("user{}", rand::random::<u32>() % record_count);
            let start = std::time::Instant::now();
            let new_value = generate_value();
            client.read_modify_write(&key, new_value).await?;

            metrics.record_read_modify_write(start.elapsed()).await;
        } else {
            // Read only operation
            let key = format!("user{}", rand::random::<u32>() % record_count);
            let start = std::time::Instant::now();
            let result = client.read(&key).await?;
            metrics.record_read(start.elapsed()).await;

            if let Some(value) = result {
                let bytes = serde_json::to_string(&value)?.len();
                total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
            }
        }

        total_operations.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}
