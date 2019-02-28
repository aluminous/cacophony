use crate::api;
use crate::scheduler::*;
use actix::{System, SystemService};
use futures::Future;
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use uuid::Uuid;

pub const TEST_JOB_SPEC: &str = "version: '3'
services:
    example-service:
        image: alexwhen/docker-2048
        ports:
        - 8080:80";

/// Run the provided function in a single-node cluster.
pub fn with_bootstrap_node<F, N, E>(f: F)
where
    F: FnOnce() -> N + 'static,
    N: Future<Item = (), Error = E> + 'static,
    E: Debug,
{
    let bind_addr = "127.0.0.1:9000";
    let node = Node::new(Uuid::new_v4().to_string(), bind_addr);
    with_node(bind_addr, move || {
        Scheduler::from_registry()
            .send(SchedulerCommand::BootstrapNode(node))
            .then(move |res| {
                res.unwrap().expect("Failed to bootstrap test node");
                f()
            })
    });
}

/// Run the provided function in a single-node cluster.
pub fn with_node<F, N, E, S>(node_addr: S, f: F)
where
    F: FnOnce() -> N + 'static,
    N: Future<Item = (), Error = E> + 'static,
    E: Debug,
    S: ToSocketAddrs,
{
    let bind_addr = node_addr
        .to_socket_addrs()
        .expect("Illegal bind addresss")
        .next()
        .unwrap();
    actix::run(move || {
        api::create_api(api::APIConfig { bind_addr });
        f().then(|res| {
            res.unwrap();
            System::current().stop();
            Ok(())
        })
    });
}
