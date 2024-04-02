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