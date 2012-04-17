# Overview

## What is Dempsy?

In a nutshell, Dempsy is a framework that provides for the easy implementation Stream-based, Real-time, BigData applications.

Dempsy is the Nokia's "Distributed Elastic Message Processing System."
* Dempsy is _Distributed_. That is to say a dempsy application can run on multiple JVMs on multiple physical machines.
* Dempsy is _Elastic_. That is, it is relatively simple to scale an application to more (or fewer) nodes. This does not require code or configuration changes but allows the dynamic insertion and removal of processing nodes.
* Dempsy is _Message Processing_. Dempsy fundamentally works by message passing. It moves messages between Message processors, which act on the messages to perform simple atomic operations such as enrichment, transformation, or other processing. Generally an application is intended to be broken down into more smaller simpler processors rather than fewer large complex processors.
* Dempsy is a _Framework_. It is not an application container like a J2EE container, nor a simple library. Instead, like the [Spring Framework|http://www.springsource.org] it is a collection of patterns, the libraries to enable those patterns, and the interfaces one must implement to use those libraries to implement the patterns.

## What Problem is Dempsy solving?

Dempsy is not designed to be a general purpose framework, but is intended to solve a certain class of problems while encouraging the use of the best software development practices.

Dempsy is meant to solve the problem of processing large amounts of "near real time" stream data with the lowest lag possible; problems where latency is more important that "guaranteed delivery." This class of problems includes use cases such as:
* Real time monitoring of large distributed systems
* Processing complete rich streams of social networking data
* Real time analytics on log information generated from widely distributed systems
* Statistical analytics on real-time vehicle traffic information on a global basis

It is meant to provide developers with a tool that allows them to solve these problems in a simple straightforward manner by allowing them to concentrate on the analytics themselves rather than the infrastructure. Dempsy heavily emphasizes "separation of concerns" through "dependency injection" and out of the box supports both Spring and Guice. It does all of this by supporting what can be (almost) described as a "distributed actors model."

In short Dempsy is a framework to enable decomposing a large class of message processing applications into flows of messages to relatively simple processing units implemented as [POJOs](http://en.wikipedia.org/wiki/Plain_Old_Java_Object)

### What is a Distributed Actor Framework?

Dempsy has been described as a distributed actor framework. While not strictly speaking an [actor](http://en.wikipedia.org/wiki/Actor_model) framework in the sense of [Erlang](http://www.erlang.org) or [Akka](http://akka.io) actors, in that actors typically direct messages directly to other actors, the Message Processors in Dempsy are "actor like POJOs" similar to Processor Elements in [S4](http://s4.io) and less so like Bolts in [Storm](https://github.com/nathanmarz/storm). Message processors are similar to actors in that Message processors act on a single message at a time, and need not deal with concurrency directly. Unlike actors, Message Processors also are relieved of the the need to know the destination(s) for their output messages, as this is handled inside the Dempsy Distributor.

The Actors model is an approach to concurrent programming that has the following features:

* **Fine-grained processing**

A traditional (linear) programming model processes input sequentially, maintaining whatever state is needed to represent the entire input space. In an Actor model, input is divided into messages and distributed to a large number of independent actors. An individual actor maintains only the state needed to process the messages that it receives.

* **Shared-Nothing**

Each actor maintains its own state, and does not expose that state to any other actor. This eliminates concurrency bottlenecks and the potential for deadlocks. Immutable state (eg, a road network artifact) may be shared between actors.

* **Message-Passing**

Actors communicate by sending immutable messages to one-another. Each message has a key, and the framework is responsible for directing the message to the actor responsible for that key.

A distributed actors model takes an additional step, of allowing actors to exist on multiple nodes in a cluster, and supporting communication of messages between nodes. It adds the following complexities to the Actors model:

* **Distribution of Messages**

A message may or may not be consumed by an actor residing in the same JVM as the actor that sent the message. The required network communication will add delay to processing, and require physical network configuration to support bandwidth requirements and minimize impact to other consumers.

* **Load-Balancing**

The framework must distribute work evenly between nodes, potentially using different strategies for different message types (eg: regional grouping for map-matcher, simple round-robin for vehicles).

* **Node Failure**

If a node fails, the workload on that node must be shifted to other nodes. All state maintained by actors on the failed node is presumed lost.

* **Network Partition**

If the network connection to a node temporarily drops, it will appear as a node failure to other nodes in the cluster. The node must itself recognize that it is no longer part of the cluster, and its actors must stop sending messages (which may conflict with those sent by the cluster's "replacement" node).

* **Node Addition**

To support elastic scalability (adding nodes on demand to service load, as well as re-integration of a previously failed node), the framework must support redistribution of actors _and their state_ based on changes to the cluster.

