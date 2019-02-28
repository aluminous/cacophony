# Cacophony

A simple distributed container scheduler in Rust.

Features include:
* REST API to create, edit, scale and delete jobs/services
* Ability to issue API requests to any node
* Scheduling weighted according to available resources on each node

Non-features / areas for improvement:
* High availability via leader-election / failover
* Ability to detect failed nodes and reschedule
* Service discovery mechanism

## Basic Usage

[Install Rust stable](https://rustup.rs/) (tested with 1.32.0).

Create a new cluster:
```bash
# We need to specify `bind-addr` so the first node knows what address to advertise to nodes.
cargo run -- --bind-addr 10.0.0.1:9000 bootstrap
```

Add more nodes:
```bash
# Provide the IP/port of any existing node.
cargo run -- join 10.0.0.1:9000
```

Manage jobs:
```bash
# Start a job
curl -v -X PUT -H 'Content-Type: application/json' \
    10.0.0.1:9000/cluster/job/my-job-id --data-binary @examples/example-job.yml

# Scale it
curl -v -X PUT -H 'Content-Type: application/json' \
    10.0.0.1:9000/cluster/job/my-job-id/example-service --data '{"scale": 5}'

# Delete it
curl -v -X DELETE 10.0.0.1:9000/cluster/job/my-job-id
```

## Client-facing API

* `GET /cluster/job` - List jobs
* `PUT /cluster/job/:job_id` - Create / update a job with user-specified ID
* `PUT /cluster/job/:job_id/:service_name` - Set scaling for a service (scale to 0 to stop). e.g. `{"scale": 0}`
* `DELETE /cluster/job/:job_id` - Delete a job
* `GET /cluster/resources` - List resource usage of all nodes

## Architecture

The service consists of 3 components:
* `Scheduler` - Makes scheduling decisions and manages node membership
* `Executor` - Creates/kills containers as instructed by the scheduler
* `API` - Runs on every node for client-facing and inter-node communication

`API` and `Executor` run on all members, but `Scheduler` runs only on one 'master' node.

On non-master nodes, the API will transparently forward most requests to the master node, so clients
don't need to care about which node is master.

The architecture is roughly as follows:
```
          (API Client)
               |
- - - - - - - -|- - - - - - - - - - - - - - - - -
 [Master Node] |           :       [Worker Node]
               v           :
        +-------------+    :
        | Cluster API |    :
        +-------------+    :
               |           :
         +-----------+     :
         | Scheduler |     :
         +-----------+     :
               |           :
               |--------------------+
               |           :        v
         +-----------+     :  +-----------+
         | Executor  |     :  | Executor  |
         +-----------+     :  +-----------+
               |           :        |
  - - - - - - -|- - - - - - - - - - | - - - - -
               v           :        v
       +---------------+   : +---------------+
       | Docker Engine |   : | Docker Engine |
       +---------------+   : +---------------+
```
