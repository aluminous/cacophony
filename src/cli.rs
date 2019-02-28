//! Describes accepted command line arguments.

use std::net::SocketAddr;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab_case")]
pub enum Command {
    /// Create a new cluster with this node as master
    Bootstrap,

    /// Join an existing cluster
    Join {
        /// Address of any node in the cluster to join
        join_addr: String,
    },
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab_case")]
pub struct Args {
    #[structopt(subcommand)]
    pub command: Command,

    /// Bind address for local node's API
    #[structopt(long, short, default_value = "0.0.0.0:9000")]
    pub bind_addr: SocketAddr,
}
