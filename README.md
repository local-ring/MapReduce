# MapReduce Implementation

## Test


## Structure    

We use zeromq to implement the communication between the master and the workers. ZeroMQ has serval patterns, request-reply, publish-subscribe, and push-pull. 

Here we mainly use `PUSH`-`PULL` and `PULL`-`PUSH` patterns.:

This pattern is used for parallel processing pipelines and load balancing workloads across multiple workers. It supports a one-way 1 to N flow of messages from producers to consumers.

- PUSH: Distributes messages to the next available worker in a round-robin fashion.
- PULL: Pulls messages from a PUSH socket. Workers using PULL sockets can process tasks in parallel, without knowing about each other. messages from a PUSH socket. Workers using PULL sockets can process tasks in parallel, without knowing about each other.
1. master node:
    - roles: get instructions from users, assign tasks to workers, tracks the status of the workers
    - zeromq pattern: use PUSH-PULL for sending tasks to workers(mappers)



When we initiate the cluster, we need number of mappers, number of reducers, and type of task (wordcount or inverted index) as input. We don't use remote procedure call for passing map and reduce functions to workers. Instead, we store the map and reduce functions in the script file of each worker, that is, in our real world case, each server will store the map and reduce functions in the their local file system. Since the function is not too expensive to store. Rather than tell the workers the type of task after initiating the cluster, we tell them in the beginning. The drawback is that we need to restart the cluster if we want to change the type of task. The advantage is in this way, the protocol is simpler: without setting up another control channel for the master to tell the workers the type of task, or setting up protocol for the workers to detect if they receive data or the type of task.