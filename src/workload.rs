use clap::ValueEnum;
use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(ValueEnum, Debug, Clone)]
pub enum WorkloadType {
    A, // 50% read, 50% update
    B, // 95% read, 5% update
    C, // 100% read
    D, // 95% read, 5% insert latest
    E, // 95% scan, 5% insert
    F, // 50% read, 50% read-modify-write

    // Custom workloads
    RangeScan,        // Sequential range scans
    SequentialInsert, // Sequential key inserts
    RandomInsert,     // Random key inserts
    VariableSize,     // Variable key-value size operations
    MixedOperations,  // Mix of operations (create, read, update, delete)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Workload {
    WorkloadA {
        read_proportion: f32,
        record_count: u32,
        operation_count: u32,
    },
    WorkloadB {
        read_proportion: f32,
        record_count: u32,
        operation_count: u32,
    },
    WorkloadC {
        read_proportion: f32,
        record_count: u32,
        operation_count: u32,
    },
    WorkloadD {
        read_proportion: f32,
        record_count: u32,
        operation_count: u32,
    },
    WorkloadE {
        scan_proportion: f32,
        record_count: u32,
        operation_count: u32,
    },
    WorkloadF {
        read_modify_write_proportion: f32,
        record_count: u32,
        operation_count: u32,
    },
}

impl Workload {
    pub fn get_record_count(&self) -> u32 {
        match self {
            Workload::WorkloadA { record_count, .. } => *record_count,
            Workload::WorkloadB { record_count, .. } => *record_count,
            Workload::WorkloadC { record_count, .. } => *record_count,
            Workload::WorkloadD { record_count, .. } => *record_count,
            Workload::WorkloadE { record_count, .. } => *record_count,
            Workload::WorkloadF { record_count, .. } => *record_count,
        }
    }

    pub fn get_operation_count(&self) -> u32 {
        match self {
            Workload::WorkloadA {
                operation_count, ..
            } => *operation_count,
            Workload::WorkloadB {
                operation_count, ..
            } => *operation_count,
            Workload::WorkloadC {
                operation_count, ..
            } => *operation_count,
            Workload::WorkloadD {
                operation_count, ..
            } => *operation_count,
            Workload::WorkloadE {
                operation_count, ..
            } => *operation_count,
            Workload::WorkloadF {
                operation_count, ..
            } => *operation_count,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LoadPattern {
    Sequential,
    Random,
}

#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    // General configuration
    pub record_count: u32,
    pub operation_count: u32,
    pub key_size: Option<KeySizeConfig>,
    pub value_size: Option<ValueSizeConfig>,
    pub range_size: Option<u32>, // For range scans
    pub load_pattern: LoadPattern,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySizeConfig {
    pub min_size: usize,
    pub max_size: usize,
    pub distribution: SizeDistribution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueSizeConfig {
    pub min_size: usize,
    pub max_size: usize,
    pub distribution: SizeDistribution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SizeDistribution {
    Fixed(usize),
    Uniform,
    Gaussian { mean: f64, std_dev: f64 },
}

// Functions to generate keys and values of different sizes
pub fn generate_random_key(config: &KeySizeConfig) -> String {
    let size = match &config.distribution {
        SizeDistribution::Fixed(size) => *size,
        SizeDistribution::Uniform => {
            let range = Uniform::new(config.min_size, config.max_size);
            range.sample(&mut rand::thread_rng())
        }
        SizeDistribution::Gaussian { mean, std_dev } => {
            use rand_distr::{Distribution, Normal};
            let normal = Normal::new(*mean, *std_dev).unwrap();
            normal.sample(&mut rand::thread_rng()) as usize
        }
    };

    // Generate random string of specified size
    let mut rng = rand::thread_rng();
    let chars: String = (0..size)
        .map(|_| {
            let idx = rng.gen_range(0..62);
            match idx {
                0..=9 => (b'0' + idx as u8) as char,
                10..=35 => (b'A' + (idx - 10) as u8) as char,
                _ => (b'a' + (idx - 36) as u8) as char,
            }
        })
        .collect();
    chars
}

pub fn generate_random_value(config: &ValueSizeConfig) -> serde_json::Value {
    let size = match &config.distribution {
        SizeDistribution::Fixed(size) => *size,
        SizeDistribution::Uniform => {
            let range = Uniform::new(config.min_size, config.max_size);
            range.sample(&mut rand::thread_rng())
        }
        SizeDistribution::Gaussian { mean, std_dev } => {
            use rand_distr::{Distribution, Normal};
            let normal = Normal::new(*mean, *std_dev).unwrap();
            normal.sample(&mut rand::thread_rng()) as usize
        }
    };

    // Generate random JSON value with specified size
    let random_string: String = (0..size).map(|_| rand::random::<char>()).collect();

    serde_json::json!({
        "data": random_string,
        "timestamp": chrono::Utc::now().timestamp(),
        "metadata": {
            "size": size,
            "generated": chrono::Utc::now().to_rfc3339()
        }
    })
}
