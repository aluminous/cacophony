//! REST API for the scheduler and nodes.

use crate::executor::*;
use crate::scheduler::*;
use actix::SystemService;
use actix_web::client::ClientRequestBuilder;
use actix_web::dev::HttpResponseBuilder;
use actix_web::middleware::{Logger, Middleware, Started};
use actix_web::{
    server, App, AsyncResponder, FutureResponse, HttpMessage, HttpRequest, HttpResponse, Json,
    Path, Responder,
};
use failure::Error;
use futures::future::{done, err, ok};
use futures::Future;
use std::net::SocketAddr;

/// Called by nodes which wish to announce thier presence to the cluster
fn register_node(
    req: HttpRequest<APIConfig>,
    node_id: Path<String>,
    remote_port: Json<u16>,
) -> FutureResponse<HttpResponse> {
    let info = req.connection_info();
    let mut peer_addr: SocketAddr = match info.remote() {
        Some(host) => host.parse().unwrap(),
        None => {
            return err(format_err!("Can't get peer address of {}", node_id))
                .from_err()
                .responder();
        }
    };
    peer_addr.set_port(remote_port.0);

    let node = Node::new(node_id.to_string(), peer_addr);
    Scheduler::from_registry()
        .send(SchedulerCommand::RegisterNode(node))
        .from_err()
        .and_then(|_| Ok(HttpResponse::Ok().into()))
        .responder()
}

/// Submit a new or updated job spec
fn put_job(job_id: Path<String>, raw_spec: String) -> FutureResponse<HttpResponse> {
    done(serde_yaml::from_str(&raw_spec))
        .map_err(|e| format_err!("Invalid job spec: {:?}", e))
        .and_then(move |spec| {
            Scheduler::from_registry()
                .send(SchedulerCommand::CreateJob(job_id.to_string(), spec))
                .from_err()
        })
        .from_err()
        .map(|_| HttpResponse::Ok().into())
        .responder()
}

/// Set the desired concurrency level of a particular service in a given job
fn put_service(
    path: Path<(String, String)>,
    scale: Json<ServiceConfig>,
) -> FutureResponse<HttpResponse> {
    let (job_id, service_name) = path.into_inner();
    Scheduler::from_registry()
        .send(SchedulerCommand::UpdateService(
            job_id,
            service_name,
            scale.into_inner(),
        ))
        .from_err()
        .map(|_| HttpResponse::Ok().into())
        .responder()
}

/// List submitted jobs
fn job_list(_req: &HttpRequest<APIConfig>) -> impl Responder {
    Scheduler::from_registry()
        .send(ListJobs)
        .map_err(Error::from)
        .and_then(|res| res)
        .map(|res| HttpResponse::Ok().json(res))
        .responder()
}

/// Delete a job
fn job_destroy(job_id: Path<String>) -> FutureResponse<HttpResponse> {
    Scheduler::from_registry()
        .send(SchedulerCommand::DeleteJob(job_id.into_inner()))
        .from_err()
        .map(|_| HttpResponse::Ok().into())
        .responder()
}

/// Called on nodes by the master node to send a new desired state
fn state_update(state: Json<ClusterState>) -> FutureResponse<HttpResponse> {
    Executor::from_registry()
        .send(ExecutorCommand::UpdateState(state.0))
        .from_err()
        .map(|_| HttpResponse::Ok().into())
        .responder()
}

/// List the resource usage on the current node
fn local_resources(_req: HttpRequest<APIConfig>) -> FutureResponse<HttpResponse> {
    Executor::from_registry()
        .send(GetNodeResources)
        .from_err()
        .and_then(|res| res)
        .from_err()
        .map(|res| HttpResponse::Ok().json(res))
        .responder()
}

/// List the resource usage of all nodes in the cluster
fn cluster_resources(_req: HttpRequest<APIConfig>) -> FutureResponse<HttpResponse> {
    Scheduler::from_registry()
        .send(GetClusterResources)
        .from_err()
        .and_then(|res| res)
        .from_err()
        .map(|res| HttpResponse::Ok().json(res))
        .responder()
}

#[derive(Clone)]
pub struct APIConfig {
    pub bind_addr: SocketAddr,
}

/// Handles proxying requests to the master node. On the master node itself, this is a noop. This
/// lets clients send requests to any node, without caring where the master is.
struct ForwardToMaster;

impl Middleware<APIConfig> for ForwardToMaster {
    fn start(&self, req: &HttpRequest<APIConfig>) -> actix_web::Result<Started> {
        let req = req.clone();
        Ok(
            Started::Future(
                Box::new(
                    Executor::from_registry()
                        .send(GetRemoteMaster)
                        .from_err()
                        .and_then(
                            move |master| -> Box<
                                Future<Item = Option<HttpResponse>, Error = actix_web::Error>,
                            > {
                                match master {
                                    Ok(Some(master)) => {
                                        let uri_on_master = format!(
                                            "http://{}{}{}",
                                            master,
                                            req.path(),
                                            req.query_string()
                                        );

                                        info!("Proxying request to master: {}", uri_on_master);
                                        Box::new(
                                            ClientRequestBuilder::from(&req)
                                                .uri(uri_on_master)
                                                .streaming(req.payload())
                                                .unwrap()
                                                .send()
                                                .from_err()
                                                .and_then(|res| {
                                                    Ok(Some(
                                                        HttpResponseBuilder::from(&res).finish(),
                                                    ))
                                                }),
                                        )
                                    }
                                    Ok(None) => Box::new(ok(None)),
                                    Err(e) => {
                                        error!("Cannot service cluster request: {:?}", e);
                                        Box::new(ok(Some(
                                            HttpResponse::ServiceUnavailable().finish(),
                                        )))
                                    }
                                }
                            },
                        ),
                ),
            ),
        )
    }
}

pub fn create_api(config: APIConfig) {
    let bind_addr = config.bind_addr;
    server::new(move || {
        App::with_state(config.clone())
            .middleware(Logger::default())
            .scope("/cluster", |s| {
                s.middleware(ForwardToMaster)
                    .resource("/job", |r| {
                        r.get().f(job_list);
                    })
                    .resource("/job/{job_id}", |r| {
                        r.put().with_async(put_job);
                        r.delete().with_async(job_destroy);
                    })
                    .resource("/job/{job_id}/{service_name}", |r| {
                        r.put().with_async(put_service);
                    })
                    .resource("/node/{node_id}", |r| r.post().with_async(register_node))
                    .resource("/resources", |r| r.get().with_async(cluster_resources))
            })
            .scope("/node", |s| {
                s.resource("/state", |r| {
                    r.post().with_async(state_update);
                })
                .resource("/resources", |r| {
                    r.get().with_async(local_resources);
                })
            })
    })
    .bind(bind_addr)
    .unwrap()
    .start();
}
