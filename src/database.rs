use clap::ValueEnum;

#[derive(ValueEnum, Debug, Clone)]
pub enum Database {
    #[cfg(feature = "surrealkv")]
    Surrealkv,
}
