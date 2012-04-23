
If you prefer, you can go straight to "[Getting Started](https://github.com/Dempsy/Dempsy/wiki/Getting-Started)"

# Overview

## What is Dempsy?

In a nutshell, Dempsy is a framework that provides for the easy implementation Stream-based, Real-time, BigData applications.

Dempsy is the Nokia's "Distributed Elastic Message Processing System."

* Dempsy is _Distributed_. That is to say a dempsy application can run on multiple JVMs on multiple physical machines.
* Dempsy is _Elastic_. That is, it is relatively simple to scale an application to more (or fewer) nodes. This does not require code or configuration changes but allows the dynamic insertion and removal of processing nodes.
* Dempsy is _Message Processing_. Dempsy fundamentally works by message passing. It moves messages between Message processors, which act on the messages to perform simple atomic operations such as enrichment, transformation, or other processing. Generally an application is intended to be broken down into more smaller simpler processors rather than fewer large complex processors.
* Dempsy is a _Framework_. It is not an application container like a J2EE container, nor a simple library. Instead, like the [Spring Framework](http://www.springsource.org) it is a collection of patterns, the libraries to enable those patterns, and the interfaces one must implement to use those libraries to implement the patterns.

## What Problem is Dempsy solving?

Dempsy is not designed to be a general purpose framework, but is intended to solve a certain class of problems while encouraging the use of the best software development practices.

Dempsy is meant to solve the problem of processing large amounts of "near real time" stream data with the lowest lag possible; problems where latency is more important that "guaranteed delivery." This class of problems includes use cases such as:

* Real time monitoring of large distributed systems
* Processing complete rich streams of social networking data
* Real time analytics on log information generated from widely distributed systems
* Statistical analytics on real-time vehicle traffic information on a global basis

It is meant to provide developers with a tool that allows them to solve these problems in a simple straightforward manner by allowing them to concentrate on the analytics themselves rather than the infrastructure. Dempsy heavily emphasizes "separation of concerns" through "dependency injection" and out of the box supports both Spring and Guice. It does all of this by supporting what can be (almost) described as a "distributed actors model."

In short Dempsy is a framework to enable decomposing a large class of message processing applications into flows of messages to relatively simple processing units implemented as [POJOs](http://en.wikipedia.org/wiki/Plain_Old_Java_Object)

### Features

Important features of Dempsy include:

* Built to support an “inversion of control” programming paradigm, making it extremely easy to use, and resulting in smaller and easier to test application codebases, reducing the total cost of ownership of applications that employ Dempsy.
* Fine grained message processing allows the developer to decompose complex analytics into simple small steps. 
* Invisible Scalability. While Dempsy is completely horizontally scalable and multithreaded, the development paradigm supported by Dempsy removes all scalability and threading concerns from the application developer. 
* Dynamic topologies. There is no need to hard wire application stages into a configuration or into the code. Topologies (by default) are discovered at run-time.
* Full elasticity (for May). Dempsy can handle the dynamic provisioning and decommissioning of computational resources.

Dempsy is intentionally not an “Application Server” and runs in a completely distributed manner relying on Apache ZooKeeper for coordination. In sticking with one of the development teams guiding principles, it doesn’t try to solve problems well solved in other parts of the industry. As DevOps tools become the norm in cloud computing environments, where it’s “easier to re-provision that to repair,” Dempsy defers to such systems the management of computational resources, however, being fully elastic (May 2012), it cooperates to produce true dynamic fault tolerance.

### What is a Distributed Actor Framework?

Dempsy has been described as a distributed actor framework. While not strictly speaking an [actor](http://en.wikipedia.org/wiki/Actor_model) framework in the sense of [Erlang](http://www.erlang.org) or [Akka](http://akka.io) actors, in that actors typically direct messages directly to other actors, the Message Processors in Dempsy are "actor like POJOs" similar to Processor Elements in [S4](http://s4.io) and less so like Bolts in [Storm](https://github.com/nathanmarz/storm). Message processors are similar to actors in that Message processors act on a single message at a time, and need not deal with concurrency directly. Unlike actors, Message Processors also are relieved of the the need to know the destination(s) for their output messages, as this is handled inside Dempsy itself.

The Actors model is an approach to concurrent programming that has the following features:

* **Fine-grained processing**

A traditional (linear) programming model processes input sequentially, maintaining whatever state is needed to represent the entire input space. In an "Fine Grained Actor" model, input is divided into messages and distributed to a large number of independent actors. An individual actor maintains only the state needed to process the messages that it receives.

* **Shared-Nothing**

Each actor maintains its own state, and does not expose that state to any other actor. This eliminates concurrency bottlenecks and the potential for deadlocks. Immutable state may be shared between actors.

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

## Guiding philosophy

Above all, and in many ways, Dempsy is meant to be *SIMPLE*. It doesn't try to be the solution for every problem. It tries to do one thing well and it is meant to support developers that think this way. Dempsy is built emphasizing, and built to emphasize several interrelated principles. These principles are meant to reduce the longer term total cost of ownership of the software written using Dempsy. These include:

* Separation of Concerns (SoC) - Dempsy expects the developer to be able to concentrate on writing the analytics and business logic with (virtually) no consideration for framework or infrastructure.

* Decoupling - SoC provides the means to isolate cross-cutting concerns so that code written for the Dempsy little to no (with due respect to annotations) dependence on even the framework itself. Developer's code is easily separable from the framework and, in the spirit of Dependency Injection, the framework uses the developer's code rather than the developer begin required to use the framework. This type of decoupling provides for analytics/business code that has no infrastructure concerns: no framework dependencies, no messaging code, no threading code, etc.

* Testability - All of this provides for code that's more testable in isolation from these concerns.

* "Do one thing well" - Dempsy is written to provide one service: support for the type of "Distributed Actors Model" (with all of the acknowledged caveats) programming paradigm. For this reason it does not pretend to be an Application Server. Nor does it substitute for the lack of an automated provistioning/deployment system. 

### How does Dempsy compare with the alternatives?

#### Complex Event Processing systems (CEP)

CEP is really trying to solve a different problem. If you have a large stream of data you want to mine by separating it into subsets and executing different analytics on each subset (which can including ignoring entire subsets), then CEP solutions make sense. If, however, you’re going to do the same thing to every message then you will be underutilizing the power of CEP. Underutilized functionality usually means an increased total cost of ownership, and Dempsy is ALL ABOUT reducing the total cost of ownership for systems that do this type of processing.

#### Pure Actors Model Frameworks and Languages

There are several pure ["Actors Model"](http://en.wikipedia.org/wiki/Actor_model) frameworks and languages that have been posed as alternatives for Dempsy. Dempsy is not a pure actors model and primarily solves a different problem. As described above Dempsy is primarily a routing mechanism for messages for "fine grained" actors. The reason we still (though loosely) call it an "actors model" is because Dempsy supports concurrency the way a typical Actors Model does.

#### Other Stream Processors

Dempsy emphasizes reducing the total cost of ownership of real-time analytics applications and as a direct result we feel it has some advantages over the alternatives.

First, as mentioned, Dempsy supports “fine grained” message processing. Because of this, by writing parallel use-cases in Dempsy and alternatives that don't support this programming model, we find that Dempsy leads to a lower code-line count.

Also, because of Dempsy’s emphasis on “Inversion of Control” the resulting applications are more easily testable. With the exception of annotations, Message Processors, which are the atomic unit of work in Dempsy, have no dependency on the framework itself. Every alternative we've found requires that the application be written against and written to use that framework.

Also, in following the adage to never require the system to be told something that it can deduce, the topology of a Dempsy application’s pipeline is discovered at runtime and doesn’t need to be preconfigured. This is primarily a by-product of the fact that Dempsy was designed from the ground up to be “elastic” and as a result, the topology can morph dynamically.

This means that applications with complicated topologies with many branches and merges can be trivially configured since the dependency relationship between stages is discovered by the framework.

## Now that you've decided to give it a try

* See "[Getting Started](https://github.com/Dempsy/Dempsy/wiki/Getting-Started)"
