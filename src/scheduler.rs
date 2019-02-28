//! Scheduler is responsible for allocating containers on cluster nodes according to the currently
//! submitted jobs and concurrency levels. It is only used on the master node.

use crate::executor::*;
use actix::fut::wrap_future;
use actix::prelude::*;
use actix::registry::SystemService;
use actix::spawn;
use actix_web::{client, HttpMessage};
use failure::{err_msg, Error};
use futures::future::{join_all, Future};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use serde_json;
use shiplift::builder::{ContainerOptions, ContainerOptionsBuilder};
use std::collections::HashMap;
use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

pub type AllocationId = String;
pub type NodeId = String;
pub type JobId = String;
pub type ServiceName = String;

const RESOURCE_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// A job specification (in docker-compose format)
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct JobSpec {
    pub services: HashMap<String, ServiceSpec>,
}

/// A service element within the job spec
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ServiceSpec {
    pub image: String,
    pub command: Option<String>,
    pub entrypoint: Option<String>,

    #[serde(default)]
    pub ports: Vec<String>,

    #[serde(default)]
    pub volumes: Vec<String>,

    #[serde(default)]
    pub environment: Vec<String>,
}

impl ServiceSpec {
    /// Create ContainerOptions based on this service spec
    pub fn build_container_options(&self) -> Result<ContainerOptionsBuilder, Error> {
        let mut opt = ContainerOptions::builder(&*self.image);
        opt.volumes(self.volumes.iter().map(|i| &**i).collect())
            .env(self.environment.iter().map(|i| &**i).collect());

        if let Some(cmd) = &self.command {
            opt.cmd(vec![&*cmd]);
        }

        if let Some(entrypoint) = &self.entrypoint {
            opt.entrypoint(entrypoint);
        }

        for port in &self.ports {
            let mut port = port.split(':');
            opt.expose(
                port.next().unwrap().parse()?,
                "tcp",
                port.next().unwrap().parse()?,
            );
        }

        Ok(opt)
    }
}

/// Describes the state of the cluster including all jobs and nodes.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ClusterState {
    pub jobs: HashMap<JobId, JobSpec>,
    pub services: HashMap<JobId, HashMap<ServiceName, ServiceConfig>>,
    pub nodes: HashMap<NodeId, Node>,
    pub allocations: HashMap<AllocationId, Allocation>,
    pub master_node: Option<NodeId>,
}

impl ClusterState {
    /// Get a reference to the current master node
    pub fn master_node(&self) -> Option<&Node> {
        match &self.master_node {
            Some(master_id) => Some(&self.nodes[master_id]),
            None => None,
        }
    }
}

/// Element of cluster state assignming a job's task to a node.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Allocation {
    pub allocation_id: AllocationId,
    pub node_id: NodeId,
    pub job_id: JobId,
    pub service_name: ServiceName,
}

/// Runtime configuration of job services (including concurrency level)
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ServiceConfig {
    pub scale: usize,
}

/// Element of cluster state used to describe a member of the cluster.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Node {
    pub node_id: NodeId,
    pub cluster_address: SocketAddr,
}

impl Node {
    pub fn new<S, T>(node_id: T, cluster_address: S) -> Node
    where
        S: ToSocketAddrs,
        T: Into<NodeId>,
    {
        Node {
            cluster_address: cluster_address.to_socket_addrs().unwrap().next().unwrap(),
            node_id: node_id.into(),
        }
    }
}

/// Updates the cluster state to match requested jobs / concurrency levels.
#[derive(Default)]
pub struct Scheduler {
    state: ClusterState,
    node_resources: HashMap<NodeId, NodeResources>,
    state_path: Option<PathBuf>,
}

impl Scheduler {
    /// Set this node as master in an empty cluster.
    fn bootstrap(&mut self, node: Node) -> Result<(), Error> {
        if self.state.master_node.is_some() || !self.state.nodes.is_empty() {
            return Err(err_msg("Cannot bootstrap a cluster with existing nodes."));
        }

        info!("Bootstrapping cluster as node {}", node.node_id);
        self.state.master_node = Some(node.node_id.clone());
        self.state.nodes.insert(node.node_id.clone(), node);

        Executor::from_registry().do_send(ExecutorCommand::UpdateState(self.state.clone()));

        Ok(())
    }

    /// Add / remove allocations based on current jobs and concurrency requirements.
    fn update_schedule(&mut self) {
        // Build a map of service to existing allocations
        let mut service_allocs: HashMap<_, Vec<AllocationId>> = HashMap::new();
        for alloc in self.state.allocations.values() {
            service_allocs
                .entry((&alloc.job_id, &alloc.service_name))
                .or_default()
                .push(alloc.allocation_id.clone());
        }

        // Put the changes we need to make here, since we can't modify self.state.allocations while
        // borrowed
        let mut to_remove = Vec::new();
        let mut to_add = Vec::new();

        // Used for weighted random node selection
        let nodes: Vec<_> = self.state.nodes.keys().collect();
        let node_index = WeightedIndex::new(nodes.iter().map(|id| {
            self.node_resources
                .get(*id)
                .map(|resources| resources.total_memory - resources.used_memory)
                .unwrap_or(1)
        }))
        .unwrap();

        // Compare the existing allocations with the desired concurrency of each service
        for (job_id, job_services) in &self.state.services {
            for (service_name, service) in job_services {
                let existing = service_allocs
                    .remove(&(&job_id, &service_name))
                    .unwrap_or_default();
                let diff = service.scale as isize - existing.len() as isize;

                debug!("Scheduling {}.{} -> {}", job_id, service_name, diff);

                if diff > 0 {
                    // Create new allocations
                    for _ in node_index
                        .sample_iter(&mut thread_rng())
                        .take(diff as usize)
                    {
                        to_add.push(Allocation {
                            allocation_id: Uuid::new_v4().to_hyphenated().to_string(),
                            //TODO: Intelligent node selection (RR or weighted random)
                            node_id: self.state.nodes.keys().next().unwrap().clone(),
                            job_id: job_id.clone(),
                            service_name: service_name.clone(),
                        });
                    }
                } else {
                    to_remove.extend(existing.iter().take(diff.abs() as usize).cloned());
                }
            }
        }

        // Remove any allocations that don't correspond to any service
        for allocs in service_allocs.values() {
            to_remove.extend(allocs.iter().cloned());
        }

        // Now we drop the index service_allocs and we can mutate the state
        for alloc_id in to_remove {
            self.state.allocations.remove(&alloc_id);
        }
        for alloc in to_add.drain(..) {
            self.state
                .allocations
                .insert(alloc.allocation_id.clone(), alloc);
        }

        self.save_state();
        spawn(
            self.update_nodes()
                .then(|res| check_err("Update nodes", res)),
        );
    }

    /// Send the latest state to each node.
    fn update_nodes(&self) -> impl Future<Item = (), Error = Error> {
        let update_fut: Vec<_> = self
            .state
            .nodes
            .values()
            .map(|node| {
                client::post(format!("http://{}/node/state", node.cluster_address))
                    .json(&self.state)
                    .unwrap()
                    .send()
            })
            .collect();

        join_all(update_fut)
            .from_err()
            .map(|results| info!("Sent updated state to {} node(s)", results.len()))
    }

    fn load_state(&mut self) -> Result<(), Error> {
        if let Some(path) = &self.state_path {
            info!("Loading state from: {:?}", path);
            let raw_state = fs::File::open(path)?;
            self.state = serde_json::from_reader(raw_state)?;
            self.update_schedule();
        }
        Ok(())
    }

    fn save_state(&mut self) {
        if let Some(path) = &self.state_path {
            info!("Saving state to: {:?}", path);
            match serde_json::to_string(&self.state) {
                Ok(serialized) => match fs::write(path, serialized) {
                    Ok(_) => {}
                    Err(e) => error!("Failed to write state: {:?}", e),
                },
                Err(e) => error!("Failed to serialize state: {:?}", e),
            }
        }
    }
}

impl Actor for Scheduler {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // Poll node resource usage (so we don't need to request it each time we reschedule)
        ctx.run_interval(RESOURCE_REFRESH_INTERVAL, |_, ctx| {
            let update_fut = wrap_future::<_, Self>(ctx.address().send(GetClusterResources))
                .map_err(|e, _, _| error!("Failed request resource refresh: {:?}", e))
                .map(|res, scheduler, _| match res {
                    Ok(res) => scheduler.node_resources = res,
                    Err(e) => error!("Failed to refresh node resources: {:?}", e),
                });

            ctx.spawn(update_fut);
        });
    }
}

impl Supervised for Scheduler {}

impl SystemService for Scheduler {}

/// Fire-and-forget type commands for the scheduler
#[derive(Clone, Debug)]
pub enum SchedulerCommand {
    CreateJob(JobId, JobSpec),
    DeleteJob(JobId),

    UpdateService(JobId, ServiceName, ServiceConfig),

    BootstrapNode(Node),
    RegisterNode(Node),

    SetStatePath(PathBuf),
}

impl Message for SchedulerCommand {
    type Result = Result<(), Error>;
}

impl Handler<SchedulerCommand> for Scheduler {
    type Result = Result<(), Error>;

    fn handle(&mut self, cmd: SchedulerCommand, _: &mut Context<Self>) -> Self::Result {
        debug!("Scheduler handling command: {:?}", cmd);
        match cmd {
            SchedulerCommand::CreateJob(job_id, job) => {
                job.services.keys().for_each(|service_name| {
                    self.state
                        .services
                        .entry(job_id.clone())
                        .or_default()
                        .insert(service_name.clone(), ServiceConfig { scale: 1 });
                });
                self.state.jobs.insert(job_id, job);
                self.update_schedule();
                Ok(())
            }
            SchedulerCommand::UpdateService(job_id, service_name, service_config) => {
                let result = self
                    .state
                    .services
                    .get_mut(&job_id)
                    .and_then(|services| services.get_mut(&service_name))
                    .map(|service| {
                        *service = service_config;
                        {}
                    })
                    .ok_or_else(|| err_msg("Error does not exist"));
                self.update_schedule();
                result
            }
            SchedulerCommand::DeleteJob(job_id) => {
                self.state.jobs.remove(&job_id);
                self.state.services.remove(&job_id);
                self.update_schedule();
                Ok(())
            }
            SchedulerCommand::BootstrapNode(node) => self.bootstrap(node),
            SchedulerCommand::RegisterNode(node) => {
                self.state.nodes.insert(node.node_id.clone(), node);
                spawn(
                    self.update_nodes()
                        .map_err(|e| error!("Failed to update new node: {}", e)),
                );
                Ok(())
            }
            SchedulerCommand::SetStatePath(path) => {
                self.state_path = Some(path);
                self.load_state()
            }
        }
    }
}

/// Message type for requesting resource usage of all nodes
pub struct GetClusterResources;

impl Message for GetClusterResources {
    type Result = Result<HashMap<String, NodeResources>, Error>;
}

impl Handler<GetClusterResources> for Scheduler {
    type Result = ResponseFuture<HashMap<String, NodeResources>, Error>;

    fn handle(&mut self, _: GetClusterResources, _: &mut Context<Self>) -> Self::Result {
        let node_queries: Vec<_> = self
            .state
            .nodes
            .values()
            .map(|node| {
                let node_id = node.node_id.clone();
                client::get(format!("http://{}/node/resources", node.cluster_address))
                    .finish()
                    .unwrap()
                    .send()
                    .map_err(Error::from)
                    .and_then(|res| res.json().from_err())
                    .map(move |res| (node_id, res))
            })
            .collect();

        Box::new(
            join_all(node_queries)
                .map(|mut res| res.drain(..).collect())
                .from_err(),
        )
    }
}

/// Message type for requesting the current list of jobs
pub struct ListJobs;

impl Message for ListJobs {
    type Result = Result<HashMap<String, JobSpec>, Error>;
}

impl Handler<ListJobs> for Scheduler {
    type Result = Result<HashMap<String, JobSpec>, Error>;

    fn handle(&mut self, _: ListJobs, _: &mut Context<Self>) -> Self::Result {
        Ok(self.state.jobs.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::scheduler::*;
    use crate::test_support::*;
    use serde_yaml;

    #[test]
    fn test_create_job() {
        let job: JobSpec =
            serde_yaml::from_str(TEST_JOB_SPEC).expect("Failed to parse sample job spec");

        with_bootstrap_node(|| {
            Scheduler::from_registry()
                .send(SchedulerCommand::CreateJob(String::from("test-job"), job))
                .and_then(move |res| {
                    assert!(res.is_ok());
                    Scheduler::from_registry().send(ListJobs)
                })
                .map(|res| {
                    assert_eq!(res.expect("List jobs failed").len(), 1);
                })
        });
    }
}
