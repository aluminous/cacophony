extern crate actix;
extern crate actix_web;
extern crate env_logger;
#[macro_use]
extern crate failure;
extern crate futures;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate shiplift;
#[macro_use]
extern crate structopt;
extern crate sysinfo;
extern crate uuid;

use actix::{System, SystemService};
use executor::{Executor, ExecutorCommand};
use failure::Error;
use scheduler::{Node, Scheduler, SchedulerCommand};
use std::env;
use std::net::ToSocketAddrs;
use structopt::StructOpt;
use uuid::Uuid;

mod api;
mod cli;
mod executor;
mod scheduler;
#[cfg(test)]
mod test_support;

fn main() -> Result<(), Error> {
    // Enable info logging by default
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "actix_web=info,cacophony=info");
    }

    env_logger::init();
    let args = cli::Args::from_args();

    let sys = System::new("cacophony");
    let node_id = Uuid::new_v5(&Uuid::NAMESPACE_DNS, args.bind_addr.to_string().as_bytes())
        .to_hyphenated()
        .to_string();

    info!("API server starting on {}", args.bind_addr);
    api::create_api(api::APIConfig {
        bind_addr: args.bind_addr,
    });

    let state_path = format!("cacophony_state_{}.json", node_id).into();
    Executor::from_registry().do_send(ExecutorCommand::SetNodeId(node_id.clone()));

    match args.command {
        cli::Command::Bootstrap => {
            Scheduler::from_registry().do_send(SchedulerCommand::SetStatePath(state_path));
            Scheduler::from_registry().do_send(SchedulerCommand::BootstrapNode(Node::new(
                node_id,
                args.bind_addr,
            )))
        }
        cli::Command::Join { join_addr } => {
            Executor::from_registry().do_send(ExecutorCommand::JoinCluster {
                join_addr: join_addr.to_socket_addrs().unwrap().next().unwrap(),
                local_port: args.bind_addr.port(),
            })
        }
    };

    sys.run();
    Ok(())
}
