# Review of Paper: "MapReduce: Simplified Data Processing on Large Clusters"

## Introduction
Here we review the paper "MapReduce: Simplified Data Processing on Large Clusters" by Jeffrey Dean and Sanjay Ghemawat. It introduces the MapReduce programming model, a framework for processing and generating large-scale datasets on clusters of computers, and the MapReduce system that implements it. The paper was published in 2004 and has been cited over 20,000 times. 

We are going to summarize the main ideas of the paper and its impact on the field of distributed computing.

## Main Ideas

Most of computation tasks are conceptually straightforward. But when it comes to processing large-scale datasets, the complexity of the task increases, such as how to parallelize the computation, distribute the data, and handle failures.

To solve this issue, the abstractions of Map and Reduce are introduced. The Map function processes a key/value pair to generate a set of intermediate key/value pairs, and the Reduce function merges all intermediate values associated with the same intermediate key. 

This abstraction arises from the observation that many real-world tasks can be expressed in this form. For example, counting the frequency of words in a large collection of documents can be expressed as a MapReduce job. The Map function processes each document and emits a key/value pair for each word, and the Reduce function sums up the counts for each word.

Failures are common in large-scale distributed systems. The MapReduce system handles failures by re-executing failed tasks on other machines when the master node detects a failure by periodically pinging the worker nodes. The system also handles stragglers, which are slow machines that slow down the overall computation, by launching backup tasks on other machines (dynamic load balancing). This can reduce the completion time of the job significantly in some cases where the stragglers are the bottleneck.

The paper also points out that at that time the network bandwidth was the a relatively scarce resource, which I believe is even somehow true nowdays, so the system is designed to minimize the amount of data transferred over the network. It took advantage of the fact that the input data is usually stored on the local disks of the worker nodes, so the system schedules tasks to run on the same machine where the data is stored. If the data is not available locally, the system tries to schedule tasks on machines that are close to the data, in terms of network distance.

## Strengths and Weaknesses

MapReduce has the following strengths:
- Expressiveness: The MapReduce model is expressive enough to capture a wide range of data processing tasks, including filtering, joins, and aggregations, in addition to the basic map and reduce operations.
- Simplicity: The MapReduce programming model is simple and easy to understand, making it accessible to a wide range of users. It can make code simpler and smaller by hiding the complexity of parallelization, distribution, and fault tolerance. Moreover, the system is designed to be easy to deploy and operate, with minimal configuration required.
- Scalability: MapReduce can scale to process large datasets by distributing the computation across a cluster comprising of thousands of machines.

However, as the field of distributed computing evolved, it became clear that users sought more than just the ability to map and reduce; they required higher-level primitives such as filtering and joins to address a wider variety of data processing needs efficiently. Moreover, while MapReduce excelled in batch processing scenarios, it was less adept at accommodating other critical use cases at scale, such as low latency operations, graph processing, streaming data, and distributed machine learning. The realization that one could build systems to handle these scenarios on top of MapReduce, but that optimizing for specific use cases often necessitated a departure from the original MapReduce model, spurred the development of specialized frameworks like Kafka. Kafka, a scalable streaming engine, draws inspiration from the general principles of MapReduce but has evolved to meet the distinct requirements of streaming applications, with significantly different use cases and APIs.

## Impact, Related Work and Applications

MapReduce, as delineated in the paper, stands as a significant simplification and concentration of principles drawn from a wide spectrum of preceding large-scale data processing systems. This programming model automates parallel computation by imposing certain restrictions, a concept not entirely novel but refined through Google's extensive experience in handling real-world computational challenges. 

MapReduce was applied to a wide range of tasks at Google, including large-scale machine learning, Google Zeitgeist, and updating web search indexing system dynamically. 

As I learned from Internet (e.g. Hackernews), despite being proprietary to Google, the core principles of MapReduce have transcended their origins, inspiring a plethora of open-source technologies and frameworks that are widely used today. Notably, Hadoop emerged as a direct response to the publication of the MapReduce model, aiming to provide a similar capability for data processing across clusters of computers in the public domain. Spark, another influential big data processing framework, was initially conceived around the basic primitives of MapReduce. Although Spark has evolved to support a broader range of data processing tasks, the imprint of MapReduce on its design is unmistakable, particularly in operations such as exchange and collect.

References:
- MapReduce: Simplified Data Processing on Large Clusters
- [Hackernews: Does (or why does) anyone use MapReduce anymore?](https://news.ycombinator.com/item?id=39113583)
- [Hackernews: MapReduce, TensorFlow, Vertex: Google's bet to avoid repeating history in AI ](https://news.ycombinator.com/item?id=37312385)
