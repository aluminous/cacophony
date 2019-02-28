extern crate actix;
extern crate actix_web;
extern crate env_logger;
#[macro_use]
extern crate failure;
extern crate futures;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rand;
extern crate serde_yaml;
extern crate shiplift;
#[macro_use]
extern crate structopt;
extern crate sysinfo;
extern crate uuid;

use actix::System;
use executor::{Executor, ExecutorCommand};
use failure::Error;
use scheduler::{Node, Scheduler, SchedulerCommand};
use std::net::ToSocketAddrs;
use structopt::StructOpt;
use uuid::Uuid;

mod api;
mod cli;
mod executor;
mod scheduler;

fn main() -> Result<(), Error> {
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

    System::current()
        .registry()
        .get::<Executor>()
        .do_send(ExecutorCommand::SetNodeId(node_id.clone()));

    match args.command {
        cli::Command::Bootstrap => System::current().registry().get::<Scheduler>().do_send(
            SchedulerCommand::BootstrapNode(Node::new(node_id, args.bind_addr)),
        ),
        cli::Command::Join { join_addr } => {
            System::current()
                .registry()
                .get::<Executor>()
                .do_send(ExecutorCommand::JoinCluster {
                    join_addr: join_addr.to_socket_addrs().unwrap().next().unwrap(),
                    local_port: args.bind_addr.port(),
                })
        }
    };

    sys.run();
    Ok(())
}
