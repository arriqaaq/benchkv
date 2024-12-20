use crate::database::Database;
use crate::workload::LoadPattern;
use crate::workload::WorkloadType;
use clap::Parser;
use clap::ValueEnum;

#[derive(Parser, Debug)]
#[command(term_width = 0)]
pub struct Args {
    /// The database to benchmark
    #[arg(short, long)]
    pub database: Database,

    /// The type of benchmark to run
    #[arg(long, default_value = "ycsb")]
    pub benchmark_type: BenchmarkType,

    /// YCSB workload type (required for YCSB benchmark)
    #[arg(long, required_if_eq("benchmark_type", "ycsb"))]
    pub workload: Option<WorkloadType>,

    /// Number of records to load initially
    #[arg(long, default_value = "1000")]
    pub record_count: u32,

    /// Number of operations to perform
    #[arg(long, default_value = "1000")]
    pub operation_count: u32,

    /// Proportion of read operations
    #[arg(long, default_value = "0.5")]
    pub read_proportion: f32,

    /// Number of warehouses for TPC-C
    #[arg(long, default_value = "1")]
    pub warehouses: i32,

    /// Duration in seconds for TPC-C
    #[arg(long, default_value = "60")]
    pub duration_secs: u64,

    /// Number of concurrent terminals for TPC-C
    #[arg(long, default_value = "1")]
    pub terminals: i32,

    /// Number of concurrent clients
    #[arg(long, default_value = "1")]
    pub num_clients: u32,

    /// Key size configuration (min:max or fixed)
    #[arg(long)]
    pub key_size: Option<String>,

    /// Value size configuration (min:max or fixed)
    #[arg(long)]
    pub value_size: Option<String>,

    /// Range size for scan operations
    #[arg(long)]
    pub range_size: Option<u32>,

    /// Pattern for loading initial data (sequential or random)
    #[arg(long, default_value = "sequential")]
    pub load_pattern: LoadPattern,

    /// Number of worker threads for Tokio runtime [default: number of CPUs]
    #[arg(short, long, default_value="8", value_parser=clap::value_parser!(u32).range(1..))]
    pub worker_threads: u32,

    /// Thread stack size in bytes [default: 2MB]
    #[arg(long, default_value = "10485760")]
    pub thread_stack_size: Option<usize>,

    /// Maximum number of blocking threads [default: 512]
    #[arg(long, default_value = "4")]
    pub max_blocking_threads: Option<usize>,

    /// Enable IO driver [default: true]
    #[arg(long)]
    pub enable_io: Option<bool>,

    /// Enable time driver [default: true]
    #[arg(long)]
    pub enable_time: Option<bool>,
}

#[derive(ValueEnum, Debug, Clone, PartialEq)]
pub enum BenchmarkType {
    #[value(name = "ycsb")]
    Ycsb,
    #[value(name = "crud")]
    Crud,
}
