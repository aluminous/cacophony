//! The executor runs on all nodes and is resonsible for reconciling the requested state (from the
//! cluster's master), and the locally running containers.
//!
//! When a new state is received, it queries Docker Engine to see if:
//! 1) any scheduled containers are not currently running
//! 2) and running containers aren't still scheduled
//!
//! This actor is not responsible for modifying the cluster state. For that, see Scheduler.

use crate::scheduler::*;
use actix::fut::{ActorFuture, WrapFuture};
use actix::prelude::*;
use actix::registry::SystemService;
use actix_web::client;
use failure::{err_msg, Error};
use futures::future::{join_all, ok, Future};
use futures::stream::Stream;
use shiplift::builder::*;
use shiplift::Docker;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::{SocketAddr, ToSocketAddrs};
use sysinfo::{ProcessorExt, SystemExt};

// Labels to apply to containers
const LABEL_NODE_ID: &str = "com.aluminous.cacophony.node-id";
const LABEL_ALLOCATION_ID: &str = "com.aluminous.cacophony.allocation-id";

/// Details about the current resource usage of a node
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct NodeResources {
    pub total_memory: u64,
    pub used_memory: u64,
    pub cpu_usage: Vec<f32>,
}

/// Updates the local state (running containers) to match the cluster state.
#[derive(Default)]
pub struct Executor {
    state: ClusterState,
    node_id: NodeId,
    system: sysinfo::System,
}

impl Executor {
    fn join<S: ToSocketAddrs>(
        &mut self,
        join_host: S,
        local_port: u16,
    ) -> impl Future<Item = (), Error = Error> {
        let join_host = join_host.to_socket_addrs().unwrap().next().unwrap();
        info!("Attempting to join cluster at {}", join_host);
        client::post(format!(
            "http://{}/cluster/node/{}",
            join_host, self.node_id
        ))
        .json(local_port)
        .unwrap()
        .send()
        .from_err()
        .and_then(|res| {
            if res.status().is_success() {
                info!("Announced our presence to cluster.");
                Ok(())
            } else {
                Err(format_err!("Failed to join cluster: {:?}", res))
            }
        })
    }

    /// Update currently running containers to match allocated services.
    fn update_state(
        &mut self,
        state: ClusterState,
    ) -> impl ActorFuture<Item = (), Error = (), Actor = Self> {
        self.state = state;
        let docker = Docker::new();

        docker
            .containers()
            .list(
                // Get all the containers created by this node
                &ContainerListOptions::builder()
                    .filter(vec![ContainerFilter::Label(
                        String::from(LABEL_NODE_ID),
                        self.node_id.clone(),
                    )])
                    .all()
                    .build(),
            )
            .map_err(|_| ())
            .into_actor(self)
            .and_then(move |containers, executor, _ctx| {
                let desired_allocs: HashMap<&str, &Allocation> = executor
                    .state
                    .allocations
                    .values()
                    .filter(|a| a.node_id == executor.node_id)
                    .map(|a| (&*a.allocation_id, a))
                    .collect();
                let current_containers: HashMap<&str, &str> = containers
                    .iter()
                    .map(|c| (&*c.labels[LABEL_ALLOCATION_ID], &*c.id))
                    .collect();

                // Remove containers which exist but aren't part of an allocataion
                let remove_fut: Vec<_> = containers
                    .iter()
                    .filter(|container| {
                        !desired_allocs.contains_key(&*container.labels[LABEL_ALLOCATION_ID])
                    })
                    .map(|container| {
                        docker
                            .containers()
                            .get(&container.id)
                            .remove(RmContainerOptions::builder().force(true).build())
                    })
                    .collect();

                // Create containers which are allocated but aren't known to Docker
                let create_fut: Vec<_> = desired_allocs
                    .iter()
                    .filter(|(id, _)| !current_containers.contains_key(*id))
                    .map(|(_, alloc)| executor.create_container(alloc))
                    .collect();

                info!(
                    "Updating running containers: {} -> {} (create {}, kill {})",
                    containers.len(),
                    desired_allocs.len(),
                    create_fut.len(),
                    remove_fut.len()
                );

                join_all(create_fut)
                    .join(join_all(remove_fut))
                    .then(|res| check_err("Execute containers", res))
                    .into_actor(executor)
            })
    }

    /// Create and start a container for an allocation. Pulls the image if needed.
    fn create_container(
        &self,
        alloc: &Allocation,
    ) -> impl Future<Item = (), Error = shiplift::Error> {
        let docker = Docker::new();

        let service = &self.state.jobs[&alloc.job_id].services[&alloc.service_name];
        let image = if service.image.contains(':') {
            service.image.clone()
        } else {
            format!("{}:latest", service.image)
        };

        let mut labels = HashMap::new();
        labels.insert(LABEL_ALLOCATION_ID, &*alloc.allocation_id);
        labels.insert(LABEL_NODE_ID, &self.node_id);

        let pull_opts = PullOptions::builder().image(&*image).build();
        let create_opts = service
            .build_container_options()
            .unwrap()
            .labels(&labels)
            .auto_remove(true)
            .build();

        docker
            .images()
            .get(&image)
            .inspect()
            .map(move |_| info!("Image already pulled: {:?}", image))
            .or_else(move |_| {
                docker.images().pull(&pull_opts).for_each(|p| {
                    debug!("Pull: {:?}", p);
                    Ok(())
                })
            })
            .and_then(move |_| Docker::new().containers().create(&create_opts))
            .and_then(|res| Docker::new().containers().get(&*res.id).start())
    }
}

impl Actor for Executor {
    type Context = Context<Self>;
}

impl Supervised for Executor {}

impl SystemService for Executor {}

/// Fire-and-forget command messages for Executor
#[derive(Clone, Debug)]
pub enum ExecutorCommand {
    UpdateState(ClusterState),
    JoinCluster {
        local_port: u16,
        join_addr: SocketAddr,
    },
    SetNodeId(NodeId),
}

impl Message for ExecutorCommand {
    type Result = Result<(), Error>;
}

impl Handler<ExecutorCommand> for Executor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, cmd: ExecutorCommand, ctx: &mut Context<Self>) -> Self::Result {
        debug!("Executor handling command: {:?}", cmd);
        match cmd {
            ExecutorCommand::UpdateState(state) => {
                ctx.spawn(self.update_state(state));
                Box::new(ok(()))
            }
            ExecutorCommand::JoinCluster {
                local_port,
                join_addr,
            } => Box::new(self.join(join_addr, local_port)),
            ExecutorCommand::SetNodeId(node_id) => {
                self.node_id = node_id;
                Box::new(ok(()))
            }
        }
    }
}

/// Get the address of the master node (if this node is not master)
pub struct GetRemoteMaster;

impl Message for GetRemoteMaster {
    type Result = Result<Option<SocketAddr>, Error>;
}

impl Handler<GetRemoteMaster> for Executor {
    type Result = Result<Option<SocketAddr>, Error>;

    fn handle(&mut self, _: GetRemoteMaster, _: &mut Context<Self>) -> Self::Result {
        match self.state.master_node() {
            Some(master) => {
                if master.node_id == self.node_id {
                    Ok(None)
                } else {
                    Ok(Some(master.cluster_address))
                }
            }
            None => Err(err_msg("Master unknown.")),
        }
    }
}

/// Message requesting resource usage of the local node
pub struct GetNodeResources;

impl Message for GetNodeResources {
    type Result = Result<NodeResources, Error>;
}

impl Handler<GetNodeResources> for Executor {
    type Result = Result<NodeResources, Error>;

    fn handle(&mut self, _: GetNodeResources, _: &mut Context<Self>) -> Self::Result {
        self.system.refresh_system();
        Ok(NodeResources {
            total_memory: self.system.get_total_memory(),
            used_memory: self.system.get_used_memory(),
            cpu_usage: self.system.get_processor_list()[1..]
                .iter()
                .map(|p| p.get_cpu_usage())
                .collect(),
        })
    }
}

/// Logs the result of async fire-and-forget futures.
pub fn check_err<T, U>(msg: &str, res: Result<T, U>) -> impl Future<Item = (), Error = ()>
where
    T: Debug,
    U: Debug,
{
    match res {
        Ok(ok_res) => debug!("{}: {:?}", msg, ok_res),
        Err(err_res) => error!("{}: {:?}", msg, err_res),
    };

    ok(())
}

#[cfg(test)]
mod test {
    use crate::executor::*;
    use crate::test_support::*;

    #[test]
    fn test_node_resources() {
        with_node("127.0.0.1:9001", || {
            Executor::from_registry()
                .send(GetNodeResources)
                .and_then(|res| {
                    let resources = res.expect("Get resources failed");
                    assert!(resources.total_memory - resources.used_memory > 0);
                    assert!(resources.cpu_usage.len() > 0);
                    Ok(())
                })
        });
    }
}
