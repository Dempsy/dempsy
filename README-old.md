# The Dempsy Project

## Table of Contents
- [The Dempsy Project](#the-dempsy-project)
  - [Table of Contents](#table-of-contents)
- [Overview](#overview)
  - [Features](#features)
  - [What is Dempsy?](#what-is-dempsy)
  - [What Problem is Dempsy solving?](#what-problem-is-dempsy-solving)
    - [Statistical Analytics](#statistical-analytics)
    - [Features](#features-1)
    - [What is a Distributed Actor Framework?](#what-is-a-distributed-actor-framework)
    - [What Dempsy Is Not](#what-dempsy-is-not)
  - [Guiding philosophy](#guiding-philosophy)
      - [Complex Event Processing systems (CEP)](#complex-event-processing-systems-cep)
      - [Pure Actor Model Frameworks and Languages](#pure-actor-model-frameworks-and-languages)
      - [Other Stream Processors](#other-stream-processors)
- [Prerequisites](#prerequisites)
- [Simple Example - Word Count](#simple-example---word-count)
  - [What does a Dempsy application look like](#what-does-a-dempsy-application-look-like)
    - [_The Message Processor Prototype_](#the-message-processor-prototype)
    - [_Message Processor Addressing_](#message-processor-addressing)
    - [_Adaptors_](#adaptors)
    - [_Application Definition_](#application-definition)
  - [Running the example](#running-the-example)
- [Terminology](#terminology)
- [Developing Applications With Dempsy](#developing-applications-with-dempsy)
  - [The Application Developer Api](#the-application-developer-api)
    - [The _Message Processor_ Lifecycle](#the-message-processor-lifecycle)
        - [Construct](#construct)
        - [@Start](#start)
        - [Prototype Ready](#prototype-ready)
        - [clone()](#clone)
        - [@Activation](#activation)
        - [Ready](#ready)
        - [@MessageHandler](#messagehandler)
        - [@Output](#output)
        - [@Evictable](#evictable)
        - [@Passivation](#passivation)
- [Deployment and Configuration](#deployment-and-configuration)
  - [Distributed Deployment](#distributed-deployment)
  - [Application Definition](#application-definition-1)
  - [Serialization Configuration and Tuning](#serialization-configuration-and-tuning)
    - [Java Serialization](#java-serialization)
    - [Kryo Serialization](#kryo-serialization)
      - [Class Registration](#class-registration)
      - [Advanced Configuration](#advanced-configuration)
  - [Monitoring](#monitoring)
    - [Framework Monitoring Metrics](#framework-monitoring-metrics)
      - [Receive side metrics](#receive-side-metrics)
      - [Processing Metrics](#processing-metrics)
      - [Send side metrics](#send-side-metrics)
    - [Monitoring Configuration](#monitoring-configuration)
      - [Enabling various monitoring reporting back-ends](#enabling-various-monitoring-reporting-back-ends)
        - [Configuring Graphite](#configuring-graphite)
        - [Configuring Ganglia](#configuring-ganglia)
        - [Configuring CSV output](#configuring-csv-output)
        - [Configuring Console output](#configuring-console-output)
      - [Metric Naming](#metric-naming)
      - [Providing Application Specific Metrics](#providing-application-specific-metrics)
  - [Executor Configuration](#executor-configuration)
    - [Configuring the Thread Pool](#configuring-the-thread-pool)
      - [Relative configuration](#relative-configuration)
      - [Absolute Configuration](#absolute-configuration)
    - [Message Queue Limits](#message-queue-limits)
    - [Blocking and Unbounded Queueing](#blocking-and-unbounded-queueing)

# Overview

## Features

If you're already familiar with real-time stream based BigData engines, the following list of features will distinguish Dempsy from the others:

* _Fine grained "actor model":_ Dempsy provides for the fine grained distribution and lifecycle management of (potentially) millions of "actors" ("message processors" in Dempsy parlance) across a large cluster of machines allowing developers to write code that concentrates on handling individual data points in a stream without any concern for concurrency.
* _Inversion of control programming paradigm:_ Dempsy allows developers to construct these large-scale processing applications decoupled from all infrastructure concerns providing a means for simple and testable POJO implementations.
* _Dynamic Topology Autoconfiguration:_ Dempsy doesn't require the prior definition of a topology for the stream processing. Not only does it dynamically discover the topology, but an application's topology can morph at runtime. Therefore, there's no topology defining DSL or configuration necessary.
* _Integrates with available IAAS and PAAS tools:_ Dempsy doesn't pretend to be an application server or a batch processing system and therefore the provisioning, deployment and management of nodes in an application are provided by cloud management tools already part of your DevOps infrastructure. It fully integrates with various monitoring solutions including: JMX, Graphite, and Ganglia.
* _Fully elastic:_ Expanding Dempsy's cooperation with existing IAAS/PAAS tools, elasticity allows for the dynamic provisioning and deprovisioning of nodes of the application at runtime. This allows for optimizing the use of computational resources over time (Currently available in the trunk revision).

## What is Dempsy?

In a nutshell, Dempsy is a framework that provides for the easy implementation Stream-based, Real-time, BigData applications.

Dempsy was originally developed at Nokia for processing streaming GPS data from vehicles in its vehicle traffic products for its "HERE" division. It stands for "Distributed Elastic Message Processing System."

* Dempsy is _Distributed_. That is to say a dempsy application can run on multiple JVMs on multiple physical machines.
* Dempsy is _Elastic_. That is, it is relatively simple to scale an application to more (or fewer) nodes. This does not require code or configuration changes but allows the dynamic insertion and removal of processing nodes.
* Dempsy is _Message Processing_. Dempsy fundamentally works by message passing. It moves messages between Message processors, which act on the messages to perform simple atomic operations such as enrichment, transformation, or other processing. Generally an application is intended to be broken down into more smaller simpler processors rather than fewer large complex processors.
* Dempsy is a _Framework_. It is not an application container like a J2EE container, nor a simple library. Instead, like the [Spring Framework](http://www.springsource.org) it is a collection of patterns, the libraries to enable those patterns, and the interfaces one must implement to use those libraries to implement the patterns.

## What Problem is Dempsy solving?

Dempsy is not designed to be a general purpose framework, but is intended to solve two specific classes of "stream processing" problems while encouraging the use of the best software development practices. These two classes include:

### Statistical Analytics

Dempsy can be used to solve the problem of processing large amounts of "near real time" stream data with the lowest lag possible; problems where latency is more important that "guaranteed delivery." This class of problems includes use cases such as:

* Real time monitoring of large distributed systems
* Processing complete rich streams of social networking data
* Real time analytics on log information generated from widely distributed systems
* Statistical analytics on real-time vehicle traffic information on a global basis

It is meant to provide developers with a tool that allows them to solve these problems in a simple straightforward manner by allowing them to concentrate on the analytics themselves rather than the infrastructure. Dempsy heavily emphasizes "separation of concerns" through "dependency injection" and out of the box supports both Spring and Guice. It does all of this by supporting what can be (almost) described as a "distributed actor model."

In short Dempsy is a framework to enable decomposing a large class of message processing applications into flows of messages to relatively simple processing units implemented as [POJOs](http://en.wikipedia.org/wiki/Plain_Old_Java_Object)

### Features

Important features of Dempsy include:

* Built to support an “inversion of control” programming paradigm, making it extremely easy to use, and resulting in smaller and easier to test application codebases, reducing the total cost of ownership of applications that employ Dempsy.
* Fine grained message processing allows the developer to decompose complex analytics into simple small steps. 
* Invisible Scalability. While Dempsy is completely horizontally scalable and multithreaded, the development paradigm supported by Dempsy removes all scalability and threading concerns from the application developer. 
* Dynamic topologies. There is no need to hard wire application stages into a configuration or into the code. Topologies (by default) are discovered at run-time.
* Full elasticity (for May). Dempsy can handle the dynamic provisioning and decommissioning of computational resources.
* Fault Tolerant. While Dempsy doesn't manage the application state in the case of a failure, its elasticity provides fault tolerance by automatically rebalancing the load among the remaining available servers. It cooperates with IaaS auto-scaling tools by rebalancing when more servers are added.

Dempsy is intentionally not an “Application Server” and runs in a completely distributed manner relying on Apache ZooKeeper for coordination. In sticking with one of the development teams guiding principles, it doesn’t try to solve problems well solved in other parts of the industry. As DevOps tools become the norm in cloud computing environments, where it’s “easier to re-provision that to repair,” Dempsy defers to such systems the management of computational resources, however, being fully elastic (May 2012), it cooperates to produce true dynamic fault tolerance.

### What is a Distributed Actor Framework?

Dempsy has been described as a distributed actor framework. While not strictly speaking an [actor](http://en.wikipedia.org/wiki/Actor_model) framework in the sense of [Erlang](http://www.erlang.org) or [Akka](http://akka.io) actors, in that actors typically direct messages directly to other actors, the Message Processors in Dempsy are "actor like POJOs" similar to "Processor Elements" in [S4](http://s4.io) and less so like Bolts in [Storm](https://github.com/nathanmarz/storm). Message processors are similar to actors in that Message processors act on a single message at a time, and need not deal with concurrency directly. Unlike actors, Message Processors also are relieved of the the need to know the destination(s) for their output messages, as this is handled inside Dempsy itself.

The Actor model is an approach to concurrent programming that has the following features:

* **Fine-grained processing**

A traditional (linear) programming model processes input sequentially, maintaining whatever state is needed to represent the entire input space. In an "Fine Grained Actor" model, input is divided into messages and distributed to a large number of independent actors. An individual actor maintains only the state needed to process the messages that it receives.

* **Shared-Nothing**

Each actor maintains its own state, and does not expose that state to any other actor. This eliminates concurrency bottlenecks and the potential for deadlocks. Immutable state may be shared between actors.

* **Message-Passing**

Actors communicate by sending immutable messages to one-another. Each message has a key, and the framework is responsible for directing the message to the actor responsible for that key.

A distributed actor model takes an additional step, of allowing actors to exist on multiple nodes in a cluster, and supporting communication of messages between nodes. It adds the following complexities to the Actor model:

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

### What Dempsy Is Not

Dempsy is not any flavor of "[Complex Event Processing](http://en.wikipedia.org/wiki/Complex_event_processing)" (nor other names CEP may be known as, such as "[Event Processing System](http://en.wikipedia.org/wiki/Event_stream_processing)," etc.). It does not support the ability to query streams with all of the attending bells-and-whistles.

These systems have their place and if your particular use case has need of their features, then Dempsy is not the right tool for you. Dempsy aims at satisfying the particular class of use-cases in stream processing that don't require the complexity of these systems in a manner that makes these applications much easier to build and maintain.

We believe there is a large number of use-cases that Dempsy fits, but there is a large number of use-cases it does not.

## Guiding philosophy

Above all, and in many ways, Dempsy is meant to be *SIMPLE*. It doesn't try to be the solution for every problem. It tries to do one thing well and it is meant to support developers that think this way. Dempsy is built emphasizing, and built to emphasize several interrelated principles. These principles are meant to reduce the longer term total cost of ownership of the software written using Dempsy. These include:

* Separation of Concerns (SoC) - Dempsy expects the developer to be able to concentrate on writing the analytics and business logic with (virtually) no consideration for framework or infrastructure.

* Decoupling - SoC provides the means to isolate cross-cutting concerns so that code written for Dempsy will have little to no (with due respect to annotations) dependence on even the framework itself. Developer's code can be easily run separate from the framework and, in the spirit of Dependency Injection, the framework uses the developer's code rather than the developer having to use the framework. This type of decoupling provides for analytics/business code that has no infrastructure concerns: no framework dependencies, no messaging code, no threading code, etc.

* Testability - All of this provides a basis to write code that's more testable.

* "Do one thing well" - Dempsy is written to provide one service: support for the type of "Distributed Actor Model" (with all of the acknowledged caveats) programming paradigm. For this reason it does not pretend to be an Application Server. Nor does it substitute for the lack of an automated provistioning/deployment system. 

#### Complex Event Processing systems (CEP)

CEP is really trying to solve a different problem. If you have a large stream of data you want to mine by separating it into subsets and executing different analytics on each subset (which can including ignoring entire subsets), then CEP solutions make sense. If, however, you’re going to do the same thing to every message then you will be underutilizing the power of CEP. Underutilized functionality usually means an increased total cost of ownership, and Dempsy is ALL ABOUT reducing the total cost of ownership for systems that do this type of processing.

#### Pure Actor Model Frameworks and Languages

There are several pure ["Actor Model"](http://en.wikipedia.org/wiki/Actor_model) frameworks and languages that have been posed as alternatives for Dempsy. Dempsy is not a pure actor model and primarily solves a different problem. As described above Dempsy is primarily a routing mechanism for messages for "fine grained" actors. The reason we still (though loosely) call it an "actor model" is because Dempsy supports concurrency the way a typical Actor Model does.

#### Other Stream Processors

Dempsy emphasizes reducing the total cost of ownership of real-time analytics applications and as a direct result we feel it has some advantages over the alternatives.

First, as mentioned, Dempsy supports “fine grained” message processing. Because of this, by writing parallel use-cases in Dempsy and alternatives that don't support this programming model, we find that Dempsy leads to a lower code-line count.

Also, because of Dempsy’s emphasis on “Inversion of Control” the resulting applications are more easily testable. With the exception of annotations, Message Processors, which are the atomic unit of work in Dempsy, have no dependency on the framework itself. Every alternative we've found requires that the application be written against and written to use that framework.

Also, in following the adage to never require the system to be told something that it can deduce, the topology of a Dempsy application’s pipeline is discovered at runtime and doesn’t need to be preconfigured. This is primarily a by-product of the fact that Dempsy was designed from the ground up to be “elastic” and as a result, the topology can morph dynamically.

This means that applications with complicated topologies with many branches and merges can be trivially configured since the dependency relationship between stages is discovered by the framework.

# Prerequisites

You will need Java 11 (or higher).

To build an application against Dempsy you will need to add the Dempsy dependencies to your build. This should be as simple as including the following dependency in your maven ```pom.xml``` file (or the gradle equivalent).

```xml
<dependency>
   <groupId>net.dempsy</groupId>
   <artifactId>dempsy-framework.impl</artifactId>
   <version>0.16</version>
</dependency>
```

The core Dempsy jar files are deployed to the Maven Central Repository. See the section on [Understanding the Dempsy Codebase](Codebase) for a description of each of the artifacts.

You can build Dempsy applications and run them within a single VM. Once you want to run your application distributed you will need an installation of [Apache ZooKeeper](http://zookeeper.apache.org/). In a development environment, or if you are just trying out Dempsy, then you can get away with a single ZooKeeper server but we suggest following the recommendations of the ZooKeeper team.

# Simple Example - Word Count

The [Dempsy Examples](https://github.com/Dempsy/Dempsy-examples) repository has several versions of the WordCount example. You can find the final version of this tutorial's code in the [userguide-wordcount](https://github.com/Dempsy/Dempsy-examples/tree/master/userguide-wordcount) sub-project.

## What does a Dempsy application look like

In order to understand how to use Dempsy you need to understand how a Dempsy application is structured and the best way to see this is through a simple example. The "Hello World" of the BigData applications seems to be the "word counter." If you're familiar with [Hadoop](http://hadoop.apache.org/) then you probably started with the [WordCount](http://wiki.apache.org/hadoop/WordCount) example. In this simple example let's suppose we have a source of words from some text. In traditional batch based BigData systems the source for these words would be a file or perhaps already partitioned across some distributed storage. In a Dempsy application we're receiving these words in real-time through a data stream. Maybe we're getting all of the live data from a large social media application and we want to calculate the histogram of word occurrences.

### _The Message Processor Prototype_

What would the Dempsy application that accomplishes the above described feat look like? Imagine that each word from this hypothetical live stream of text is broken into its own message. Each of these messages is routed to an instance of a class (let's call that instance a _message processor_) that has the responsibility to keep track of the count for a single word. That is, there is an instance of Word Counting _message processor,_ per distinct word. For example, every time the word "Google" flows through the stream it's routed to the same _message processor_ (the one dedicated to the word "Google"). Every time the word "is" is encountered, it's routed to another _message processor_ instance. And likewise with the word "Skynet."

How easy would it be to write the code for that _message processor_ class? This way of looking at the problem makes the code fairly simple. It could be as simple as this:

```java
class WordCount {
  private long count = 0;

  public void countWord(String word) {
     count++;
  }
}
```

Notice, we write the _message processor_ in such a way that we assume each instance is responsible for a single word and that in the larger application there will be a many instances, each operating on their piece of the stream. These instances can be (usually are) spread out over a large number of machines.

Of course, what's missing? How does each `word` get to its respective _message processor_? How are the `WordCount` instances instantiated, deleted, provided their `word` message? Where are they instantiated? What about synchronization? What about sending out messages? All of these are the primary responsibility of Dempsy.

### _Message Processor Addressing_

At this point we have a nice little piece of POJO functionality completely unmoored from infrastructural concerns. Let's look at how Dempsy handles some of these concerns. As mentioned, one of Dempsy's primary responsibilities is, given a message, find the _message processor_ responsible for handling that message. Now that we have a POJO that accomplishes some business functionality we need to tell Dempsy how they are to be _addressed_, that is, how Dempsy is to find which _message processor_ is responsible for which _messages_.

The way Dempsy does this is through the use of a _message key_. Each message that flows through Dempsy needs to have a _message key_. Dempsy is (optionally) annotation driven, so classes that represent _messages_ need to identify a means of obtaining the _message's_ _MessageKey_ through the use of an annotation. An important concept to grasp here is that **a `MessageKey` is essentially the address of an individual _message processor_ instance**.

In our example, each _word_ is a message.

```java
import net.dempsy.lifecycle.annotation.MessageKey;

@MessageType
public class Word implements Serializable {
    private final String wordText;

    public Word(final String data) {
        this.wordText = data;
    }

    @MessageKey
    public String getWordText() {
        return this.wordText;
    }
}
```

So when Dempsy receives a message of type `Word`, it retrieves the _key_ using the annotated method `getWordText()`. That _key_ will become the address of a _message processor_ somewhere on the system. Dempsy will find the _message processor_ instance (in this case an instance of the class `WordCount`) within a _cluster_ of _nodes_ responsible for running the `WordCount` message processing. In the case that the instance doesn't already exist, Dempsy will `clone()` a `WordCount` instance prototype.

> Note: the annotation @MessageType is used to tell Dempsy that the class name is meant to be used 
> as a "message type" in the system. Please see the user guide for a more detailed description.

If you're paying attention you might notice there's two gaps that need to be filled in the `WordCount` implementation. First, how is it that Dempsy understands that the `WordCount` handles the `Word` message, and second, how is a `WordCount` prototype cloned (notice the existing `WordCount` class cannot (yet) simply be `cloned()`).

This requires us to revisit the `WordCount` implementation. We need to do several things to satisfy Dempsy:

1. We need to identify the `WordCount` class as an _Mp_ which is done with a class annotation
1. We need to identify the `WordCount.countWord()` call as the point where the _Mp_ handles the message by annotating it with the `@MessageHandler` annotation.
1. We need to make sure `WordCount` is _Cloneable_.

This would be accomplished by the following:

```java
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;

@Mp
public class WordCount implements Cloneable {
    private long count = 0;

    @MessageHandler
    public void countWord(final Word word) {
        count++;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}

```

The framework now has enough information that it can understand:

1. How to create instances of the `WordCount` _message processor_ given an already existing instance it will use as a _prototype_.
1. That instances of `WordCount` handle messages of type `Word` using the `WordCount.countWord()` method.
1. That the key for a given `Word` message, which represents the "address" of a unique `WordCount` instance to which the `Word` message should be routed, is provided by a call on the _message_ instance, `Word.getWordText()`.

>Note: Currently the default implementation for the Serializer is <a href="http://code.google.com/p/kryo/">Kryo</a>.
>This at least requires messages to have a default constructor defined (though Kryo supports private default
>construtor invocations under the right circumstances. See the Kryo documentation for more information). 
>Also, Dempsy can be configured with the standard Java Serializer (though I'm not sure why anyone would ever
> want to do this).

**a word about the message key**

It is critical that the Object that Dempsy obtains as the message key (in the example that would be the result of the call to `Word.getWordText()`) has the appropriate identity semantics. In all cases that means there needs to be a non-default `equals()` and `hashCode()` method. The reason for this is partially very obvious: a "unique" message key corresponds to an instance of a _message processor_ so it's important to get the understanding of "unique" correct. The default `Object` behavior is not adequate. Think of Dempsy as using the key as if it were a key in a HashMap that contained all of the current _message processor_ instances. The default implementation of `Object.equals` and `Object.hashCode` wouldn't work given multiple instantiations of the same `Word`.

But this is not all. Given that instances of a _message processor_ are distributed across many _nodes_, the default routing behavior of Dempsy uses the `hashCode()` as a means of determining which _node_ a particular _message processor_ is running on. Therefore, while strictly speaking most Java applications would work (though very poorly) if, for example, the `hashCode()` method were implemented to simply return `1`, this would cause ALL message processors to be instantiate on the same _node_ of a _cluster_.

In the example, the `MessageKey` is a `java.lang.String` which has appropriate identity semantics. &nbsp;Note: The mesage key is not restricted to only String type but to any type that is hashable.

### _Adaptors_

So where do `Word` messages come from and how do they get to Dempsy in order to be routed to the appropriate `WordCount` _message processor_? Dempsy provides an interface that needs to be implemented by the application developer in order to adapt sources of stream data to the Dempsy message bus. An `Adaptor` implementation:

1. will be given a handle to the Dempsy message bus through an interface called a `Dispatcher`.
1. will need to obtain data from an external source and use the `Dispatcher` to send that data onto Dempsy

The API for an `Adaptor` is very simple so we will extend the Word Count example with the following class:

```java
...
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;

public class WordAdaptor implements Adaptor {
    private Dispatcher dempsy;
    private AtomicBoolean running = new AtomicBoolean(false);

    /**
     * This method is called by the framework to provide a handle to the
     * Dempsy message bus. It's called prior to start()
     */
    @Override
    public void setDispatcher(final Dispatcher dispatcher) {
        this.dempsy = dispatcher;
    }

    @Override
    public void start() {
        // ... set up the source for the words.
        running.set(true);
        while (running.get()) {
            // obtain data from an external source
            final String wordString = getNextWordFromSoucre();
            if (wordString == null)
                running.set(false);
            else {
                try {
                    dempsy.dispatchAnnotated(new Word(wordString));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e); // This will stop the flow of Words from this adaptor.
                                                   //  Optimally you'd like to recover and keep going.
                }
            }
        }
    }

    @Override
    public void stop() {
        running.set(false);
    }

    private String getNextWordFromSoucre() {
        ... // get the next word to put on the processing stream
    }
}
```

When the `WordAdaptor` is registered with Dempsy, it will be provided a handle to a `Dispatcher`. Then `Adaptor.start()` will be called. The application developer is responsible for creating Dempsy compliant messages (as described above, a message should be `Serializable`  by whatever serialization technique is chosen (Kryo by default), and have a means of obtaining a `MessageKey` identified) using data from an external source.

Notice the lifecycle. The `start()` is called from the framework but it never exits. If it ever does exit, it will not be called again without restarting the node that the `Adaptor` was instantiated in. **Note: It's very important that you manage this.** You are allowed to exit the `start()` method whenever you want, either because the `Adaptor` is finished (if such a case exists) or because you decided to do the work in another thread (or many other threads) but Dempsy will **not** re-invoke the `start()` method.

Dempsy will invoke the `stop()` method to shut down the Adaptor when the node shuts down. Well behaved Adaptors *must* return from `start()` at this time, if they had not done so previously. Not doing so will hang the Vm on exit since, by default, the Adaptor is run in a non-daemon thread (though this is a configurable option for ill-behaved Adaptors).

### _Application Definition_

The examples that follow will show how to do the configuration by hand which means the translation to any DI container should be obvious to the users of those containers. We will also include Spring examples.

At this point we should begin to have an understanding of what a Dempsy application is. It's a series of instances of _message processors_ across a number of compute nodes, being routed _messages_ based on their _keys_, and being supplied _message_ data by `Adaptors`. The configuration of an application is simply a formalization of these specifics with specific infrastructure selections. To define (configure) the Word Count application we've been walking through, we need to simply lay out the specifics. Doing this programatically we would have:

```java
import java.util.concurrent.ArrayBlockingQueue;

import net.dempsy.NodeManager;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.Node;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.monitoring.dummy.DummyNodeStatsCollector;
import net.dempsy.transport.blockingqueue.BlockingQueueReceiver;

public class Main {

    public static void main(final String[] args) throws ClusterInfoException {

        @SuppressWarnings("resource")
        final NodeManager nodeManager = new NodeManager()          // 1
                .node(new Node("word-count").setClusters(
                        new Cluster("adaptor")
                                .adaptor(new WordAdaptor()),
                        new Cluster("counter")
                                .mp(new MessageProcessor<WordCount>(new WordCount()))
                                .routing("net.dempsy.router.managed"))
                        .receiver(new BlockingQueueReceiver(new ArrayBlockingQueue<>(100000)))
                        .nodeStatsCollector(new DummyNodeStatsCollector()))      // this will be defaulted in 0.9.1
                .collaborator(new LocalClusterSessionFactory().createSession());

        nodeManager.start();

        System.out.println("Exiting Main");
    }
}
```

Notice what we are NOT doing here. We are NOT defining the topology of the Dempsy application. The order the C
lusters are listed in makes no difference. In this above case we've decided to run both our `Adaptor` and our _message processor_ on the `Node`. 

The above example has all "in process" infrastructure chosen. The `LocalClusterSessionFactory` and the `BlockingQueueReceiver` will not span processes. This can be remedied by selecting the `ZookeeperSessionFactory` and the `NioReceiver` instead. 

Note also, once the distributed infrastructure is chosen (`ZookeeperSessionFactory`, `NioReceiver`) we can run the `Adaptor` on one set of nodes and the Word Processor on another. In that case you'd have 2 different Main classes (or if you're configuring with Spring, two different Spring contexts). One that starts the `Node` with just the `Adaptor` and one that started it with just the `WordCount` message processor. The topology is implicit in the business class definitions (`Word`, `WordCount`). 

The application, called "word-count," consists of two clusters, "adaptor" and "mp," the first of which contains our Adaptor which, as we have seen, sources `Word` messages. This is followed by a _message processor_ whose prototype is an instance of `WordCount`.

<div align="center">
<table align="center" width="70%" border="1" >
<tr><td>
<img width="100%" src="https://raw.github.com/wiki/Dempsy/Dempsy/images/Dempsy-UserGuide-WordCount.png" alt="WordCount pipline so far" />
</td>
</tr>
<tr><td><center>Fig. 1 Message Processor Lifecycle</center></td></tr>
</table>
</div>

Although messages coming from the `WordAdaptor` flow to the `WordCount` _message processor_, the order in the definition doesn't actually matter. Dempsy determines where _messages_ are sent based on the type of the _message_ and the type of object that the `MessageHandler` on the `MessageProcessor` takes. In the case of our example, when the `WordAdaptor` _adaptor_ produces a message of type `Word`, Dempsy knows that message can be handled by the `WordCount` _message processor_ because the method `WordCount.countWord()` (which is annotated with the `@MessageHandler` annotation) takes the type `Word`. If there are other _message processors_ that also have handlers that take a `Word` the messages will be routed to the appropriate _message processor_ within those _clusters_ also.

What do we do with the `ApplicationDefinition`? That depends on the Dependency Injection framework you're using. If using either Spring or Guice you don't need to do much else to run your application. If you're using a different dependency injection container then you'll need to obtain a reference to the `Dempsy` object and give it the `ApplicationDefinition`, but this is a more advanced topic for a later section. Moving forward we will show you how the Spring implementation works.

The above application definition could be defined using Spring as follows:

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="word-count" />
    <property name="clusterDefinitions">
      <list>
        <bean class="com.nokia.dempsy.config.ClusterDefinition">
          <constructor-arg value="adaptor"/>
          <property name="adaptor">
            <bean class="com.nokia.dempsy.example.userguide.wordcount.WordAdaptor" />
          </property>
        </bean>
        <bean class="com.nokia.dempsy.config.ClusterDefinition">
          <constructor-arg value="mp"/>
          <property name="messageProcessorPrototype">
            <bean class="com.nokia.dempsy.example.userguide.wordcount.WordCount"/>
          </property>
        </bean>
      </list>
    </property>
  </bean>
</beans>
```

## Running the example

By default there are two modes that you can run the Dempsy application in. It can be run all within a local Java VM. Or it can be run distributed on a set of machines. For the purposes of this tutorial we will demonstrate how to run it in a local Java VM. This would be easy to set up in an IDE like Eclipse.

There are several "main" implementations provided depending on what mode you're running a Dempsy application in, as well as which DI framework you're using. As a matter of fact the only place any particular DI container is assumed is in these supplied "main" applications so adding other currently unsupported DI containers is straightforward.

To run your application using the Spring container on the command line you would use:

```bash
java -Dapplication=WordCount.xml -cp [classpath] com.nokia.dempsy.spring.RunAppInVm
```

Your classpath will need to contain all of the main Dempsy artifacts plus the Dempsy Spring library. See the section on the codebase structure for more information.

# Terminology

Having gone through the [example](Simple-Example) we should codify some of the terminology that was developed and will continue to be used throughout this document as well as throughout the Dempsy code.

<table>
<tr><th>Term</th><th>Definition</th>
</tr>
<tr>
<td> message processor </td><td> an instance of a cloned <em>message processor prototype</em> responsible for processing every <em>message</em> of a particular type with a particular unique <em>key</em>. </td>
</tr>
<tr>
<td> message processor prototype </td><td> an instance used by Dempsy to serve as a template when it needs to create more instances of a <em>message processor</em>. </td>
</tr>
<tr>
<td> message </td><td> is an object that Dempsy routes to a <em>message processor</em> based on the <em>message</em>'s <em>key</em>. </td>
</tr>
<tr>
<td> key </td><td> obtained from a <em>message</em> using the method on the <em>message</em> object that's annotated with the `@MessageKey` annotation. Each unique <em>key</em> addresses an individual <em>message processor</em> instance in a <em>cluster</em> </td>
</tr>
<tr>
<td> cluster </td><td> a <em>cluster</em> is the collection of all <em>message processors</em> or <em>adaptors</em> of a common type in the same stage of processing of a Dempsy application. A <em>cluster</em> contains a complete set of potential <em>message processors</em> keyed by all of the potential <em>keys</em> from a particular <em>message</em> type.That is, a <em>cluster</em> of <em>message processor</em> instances covers the entire <em>key</em>-space of a <em>message</em>. </td>
</tr>
<tr>
<td> node </td><td> a <em>node</em> is a subset of a set of <em>cluster</em>s containing a portion of the each <em>cluster's message processors</em>. <em>nodes</em> are almost always (except in some possible test situations) the intersection of a <em>cluster</em> and a Java process. That is, the portion of a <em>cluster</em> that's running in a particular process is the <em>cluster's node</em> </td>
</tr>
<tr>
<td> container </td><td> Sometimes also referred to as a <em>message processor container</em>, it is the part of the Dempsy infrastructure that manages the lifecycle of <em>message processors</em> within an individual <em>node</em>. That is, there is a one-to-one between the portion of a cluster running in a particular <em>node</em>, and a <em>container</em>. </td>
</tr>
</table>

# Developing Applications With Dempsy

Dempsy recognizes three types of developers and the codebase is organized accordingly. 

1)"Application developers" are those that are only concerned with building real-time stream based analytics.

2) However, Dempsy is built on a set of abstractions that allow it to be extended with new transports, routing strategies, monitoring techniques, as well as others. Developers interested in adding new implementations or techniques are "framework developers." 

3) Any developer changing the existing framework or any of the default implementations of the core abstractions are the Dempsy contributors.

The Dempsy codebase is broken into `jar` artifacts that reflect this understanding of the developer community. These artifacts are served from the Maven Central Repository and so should be accessible to any modern build tool (maven, gradle, ivy). The  core codebase is broken into these three layers:

*  The Application Developer Api (lib-dempsyapi). The jar artifact _lib-dempsyapi_ contains all of the annotations, interfaces, exceptions and configuration classes, required for the application developer. To use it your build system should refer to:

```xml
            <groupId>net.dempsy</groupId>
            <artifactId>lib-dempsyapi</artifactId>
```    

* The Dempsy Framework Api (lib-dempsycore). The jar artifact _lib-dempsycore_ contains the the set of core abstractions that Dempsy itself is built on internally. It's meant as an api for those that want to extend the framework itself. For example, if someone doesn't like the current message transport implementations that comes out-of-the-box with Dempsy, they can implement their own and plug it in. This artifact includes `lib-dempsyapi` as a transitive dependency and to use it your build system should refer to:

```xml
            <groupId>net.dempsy</groupId>
            <artifactId>lib-dempsycore</artifactId>
```    
 
* The Default Dempsy Implementation (lib-dempsyimpl). The jar artifact _lib-dempsyimpl_ contains the default implementations for the framework Api (from lib-dempsycore) as well as a set of required concrete classes. This artifact includes `lib-dempsycore` as a transitive dependency and to use it your build system should refer to:

```xml
            <groupId>net.dempsy</groupId>
            <artifactId>lib-dempsyimpl</artifactId>
```    

Dempsy also currently includes two runtime libraries that each support the startup and configuration using a particular Dependency Injection container.

* Spring based startup and configuration (lib-dempsyspring). This library contains the "main" methods for starting Dempsy using Spring. The code here assumes that the `ApplicationDefinition` will be accessible to the Spring application context. This artifact includes `lib-dempsyimpl` as a transitive dependency and to use it your build system should refer to:

```xml
            <groupId>net.dempsy</groupId>
            <artifactId>lib-dempsyspring</artifactId>
```    

* _Note: The Guice support is currently under construction:_ Guice based startup and configuration (lib-dempsyguice). This library contains the "main" methods for starting Dempsy using Google Guice. The code here assumes that the `ApplicationDefinition` will be accessible to the Guice application module. This artifact includes `lib-dempsyimpl` as a transitive dependency and to use it your build system should refer to:

```xml
            <groupId>net.dempsy</groupId>
            <artifactId>lib-dempsyguice</artifactId>
```    
## The Application Developer Api

`lib-dempsyapi` contains all of the classes, interfaces, and annotations that an "application developer" would need. Some of these we've seen already. A table with brief overview follows. These are expanded upon in more detail in later sections.

<table>
<tr>
<th> Element </th><th> Description </th>
</tr>
<tr>
<td> <a href="Simple-Example#wiki-messageprocessoraddressing">@MessageProcessor</a> </td><td> This is a class annotation that identifies the class of a <em>message processor prototype</em> </td>
</tr>
<tr>
<td> <a href="Simple-Example#wiki-messageprocessoraddressing">@MessageHandler</a> </td><td> This is a method annotation that identifies a method on a <em>message processor prototype</em> class (which must be annotated with a @MessageProcess annotation) as one that handles a message. The method so annotated must take a single object class which must follow the Dempsy <em>message</em> requirements and can also return a <em>message</em> if desired. </td>
</tr>
<tr>
<td> <a href="Simple-Example#wiki-messageprocessoraddressing">@MessageKey</a> </td><td> As described above, a Dempsy message class requires that one, and only one, method be annotated as a <em>message key</em>. </td>
</tr>
<tr>
<td> <a href="Activation">@Activation, @Passivation</a> </td><td> When Dempsy instantiates a <em>message processor</em> to handle a particular message, it's first @Activated and provided the message key. Also, when Dempsy removes a <em> message processor </em> it will @Passivate it first. </td>
</tr>
<tr>
<td> <a href="Output">@Output </a> </td><td> Dempsy can make calls on <em>message processors</em> based on a particular schedule rather than simply based on incoming data. Methods annotated with the @Output call will be invoked at these scheduled times, and the data they return, if a compliant Dempsy <em>message</em>, will be passed on </td>
</tr>
<tr>
<td> <a href="Eviction">@Evictable</a> </td><td> Dempsy can remove unused message processors from the container. This behavior is fully controlled by the application developer</td>
</tr>
<tr>
<td> <a href="Simple-Example#wiki-adaptors">Adaptor</a> </td><td> The <em>Adaptor</em> interface is implemented by the application developer to adapt Dempsy to external sources of data.</td>
</tr>
<tr>
<td> <a href="Pre-instantiation">KeySource</a> </td><td> Dempsy can pre-create all <em>message processors</em> at start-up rather than simply based on incoming data. The application developer will implement the <em>KeySource</em> interface to provide the domain of the key-space to Dempsy.</td>
</tr>
</table>

### The _Message Processor_ Lifecycle

The core of a Dempsy application is the Message Processor. All of the features of Dempsy can be understood by walking through the lifecycle of a message processor. The following diagram is the reference for the rest of the section which will walk through each phase and transition.

<div align="center">
<table align="center" width="70%" border="1" >
<tr><td>
<img width="100%" src="https://raw.github.com/wiki/Dempsy/Dempsy/images/MPLifecycle.png" alt="Message Processor Lifecycle" />
</td>
</tr>
<tr><td><center>Fig. 1 Message Processor Lifecycle</center></td></tr>
</table>
</div>

##### Construct
First, the _message processor prototype_ is instantiated (constructed) on start-up of the node. You therefore have access to all of the DI framework capabilities for configuring your prototype.

In the diagram the lifecycle stages of the _message processor prototype_ (as opposed to the _message processor_) are the four stages in the top center of the diagram.

Keep in mind the distinction between a _message processor prototype_ and a _message processor_. There is a reason this is called a _prototype_. The instance that's instantiated on start-up by the dependency injection framework serves as a template for future instances. Dempsy will use the "`clone`" method to create more instances from this prototype. You need to consider that when you configure and initialize your _message processor prototype_ and when you write the "`clone`" method.

Also note, there will be a prototype created within each _node_ that the specific _message processor_ will be running in. However, it is possible that instances within one _node_ were instantiated from cloning a prototype within another node and then that instance was migrated as part of Dempsy's elastic behavior. This could affect you if you rely on static members in your classes or on references between _message processors_ and the _message processor prototype_.

##### @Start 

Dempsy provides an opportunity to initialize the _message processor prototypes_ in a phase of the lifecycle other than construction that will only be invoked in _nodes_ where the prototype will be used. Any method annotated with @Start will be called after construction. You can think of this as a @PostContruct for the _prototype_. 

<table border="1"><tr><td>
Note: we intend to introduce "lazy construction" of <em>message processor prototypes</em> which will remove the need for the @Start annotation and lifecycle phase of the <em>prototype</em>. To keep up with the progress and decisions on this issue, or participate in the discussion, see the "<a href="https://github.com/Dempsy/Dempsy/issues/4">Lazy instantiation of Mp Prototypes</a>" issue.
</td></tr></table>

The method annotated with the `@Start` is allowed to take a `ClusterId` which will be passed by the framework when it is invoked with the value that corresponds to the Application Name and Cluster Name where this _prototype_'s @Start method is being invoked.

##### Prototype Ready

After the @Start, or after the Construction if there is no method annotated with an @Start, the _message processor prototype_ enters the Prototype Ready phase where it will now serve as a template to create _message processors_ as needed.

##### clone()

From the Prototype Ready state, the "clone()" method can be invoked for one of two reasons.

1. When a message comes into the node for with a key for a _message processor_ that has not yet been created, then ```clone()``` will be invoked on the prototype.
1. There is a means of [pre-instantiating](Pre-instantiation) the entire _key space_ in a cluster. Please see the section on [pre instantiation of message processors](Pre-instantiation).

##### @Activation

The _message processor_ resulting from the ```clone``` method call is then _activated_ by having any method annotated with the @Activation annotation invoked. If the @Activation method takes a parameter of the type of the _message key_ then the key is passed to the @Activation method.

@Activation annotated method is called during [pre-instantiation](Pre-instantiation) process on each Message Processor clone.

While not shown on this diagram, @Activation will also be called when a prototype is migrated from one node to another. See the section on [elasticity] and to corresponding diagram.

See the section on [Activation and Passivation](Activation) for more details.

##### Ready

In this state the _message processor_ is available to be used in normal processing. It can now be used to handle messages, @Output directives (see below), or @Evictable checks (see below). If the _message processor_ was  created in response to an incoming message, it will immediately be given the message to work on and enter the @MessageHandler phase.

##### @MessageHandler

When a message comes in for the _message processor_ then the appropriate method annotated with the @MessageHandler annotation will be invoked and given the message. Any resulting data will be routed on to any _cluster_ capable of receiving the message.

##### @Output

Dempsy will periodically invoke any method on a _message processor_ that's annotated with the @Output annotation. The schedule for @Output calls is configurable as part of the `ClusterDefinition`. 

`@Output` provides a mechanism for message processors to add data to the Dempsy processing chain without it being driven from the stream itself. Any messages returned from the invocation of the _message processor_ @Output method will be routed on to any clusters that handle those messages.

<table border="1"><tr><td>
Note: The @Output scheduler is currently supported but will be changed in the future to be "cron"' like rather than using relative time intervals. You can keep up with this development or comment on it by watching the enhancement issue "<a href="https://github.com/Dempsy/Dempsy/issues/10">Cron-like output scheduler</a>."
</td></tr></table>

See the section on [Non-stream Driven Message Processor Output](Output) for more details.

##### @Evictable

In some use cases, there may be a desire for _message processor_ instances to be transient. For example, in a case where message processors are monitoring events from an individual session with a unique id. Once that session closes the message processor will never be invoked again, but will remain in the container.

For such cases Dempsy provides a means for the _message processor_ to let the container know that it can be evicted from the container. If the _message processor_ annotates a method with the @Evictable annotation that returns a `boolean` then the container will periodically invoke that method. If that method ever returns `true`, an indication that the _message processor_ instance can be removed from the container, then that _message processor_ will never have the message handler or @Output method invoked again, and will eventually be cleaned up by garbage collection.

##### @Passivation

When a _message processor_ is going to be removed from a container, the message processor is notified via any method that is annotated with the @Passivation annotation. This phase of the lifecycle provides the _message processor_ with a clean means of managing its state.

There are two events that will cause a _message processor_ to have the `@Passivation` method invoked.

1. As described above, the _message processor_ is being evicted from the container.
1. The @Passivation method is called when a _message processor_ is about to be migrated to another node in the cluster.

Keep in mind that in cases where an application has stateful message processors, Dempsy relies on the application to manage that state. While the @Passivation method can be helpful, it cannot be relied upon as the sole point where stateful applications will persist their state information. In the case of node failures @Passivation is not invoked, but the recreation and @Activation of that _message processor_ can still happen on another node following such a failure.

# Deployment and Configuration

Obviously, Dempsy is meant to be deployed in a distributed manner. It's designed to make this really easy, yet not take on responsibilities better solved by other infrastructure components. By this we mean that Dempsy does NOT operate as an "Application Server." You do not "deploy" an application to Dempsy. Rather you deploy your application, along with Dempsy.

There is a significant difference between batch oriented big-data systems and stream based big-data systems. In batch oriented big-data (Hadoop) you have data partitioned among nodes and you send the functionality to the data in order to operate on it in parallel. Therefore, in cases like that, the concept of "deploying" and application to an application server makes sense.

Here, however, your functions run, and the job of the system is to get the data to your code. This means there can be a better separation of concerns and well established IAAS cloud based provisioning tools can be used to manage the system.

Dempsy is designed to be run in a cloud based system and rely on the provisioning tools already supporting the cloud. On Amazon, for example, you can automatically provision more nodes in response to load (using Amazon's "CloudWatch"). 

Dempsy is responsible for the coordination of nodes, once they are started. Not for starting them. But Dempsy makes this coordination work with almost no effort or configuration. The next sections describe how this works.

## Distributed Deployment

In order to distribute a Dempsy application on a set of nodes, you simply start the application using the `com.nokia.dempsy.spring.RunNode` wrapper with a few system parameters ... on all of the nodes. Some of the parameters are required and some are optional. 

```bash
java -classpath classpath [options] com.nokia.dempsy.spring.RunNode
```

options include:

1. -Dappdef - <em>Required</em> - This identifies the spring application context with the [ApplicationDefinition](Simple-Example#wiki-applicationdefinition) describing the application being started.
1. -Dapplication - <em>Required</em> - This parameter identifies the application and cluster to start by name. The format is: `application:cluster`. Both `application` and `cluster` can be regular expressions. This provides for the ability to start multiple clusters within the same node.
1. -Dzk_connect - <em>Required</em> - As mentioned in the [Prerequisites](Prerequisites) section, an [Apache ZooKeeper](http://zookeeper.apache.org/) install is required to run Dempsy in a distributed manner. The connect string for ZooKeeper needs to be supplied.
1. -Dzk_session_timeout - <em>Optional</em> - Please refer to the ZooKeeper documentation for the description of the <em>timeout</em> parameter. The value is in milliseconds and the default is 5000 (milliseconds).
1. -Dmin_nodes_for_cluster - <em>Optional</em> - This parameter should be set to the minimum number of nodes it will require to run the application. If the number of nodes falls below this number, then some of the messages being sent to this cluster (or these clusters) will be dropped. The default is 1.
1. -Dtotal_slots_for_cluster - <em>Optional</em> - This allows the tunning of the number of `shards` being used in the node management. Please see the section on Routing for more details. The default is 100.

That's it. There's no configuration about what is running where as this is discovered dynamically. There's no configuring of a physical topology and even the logical topology (what messages from what processors go to what other processors) is discovered at runtime and can change dynamically (see the sections on Routing and Dynamic Topology for more details).

As a result of this flexibility Dempsy cooperates with applications that run in a cloud environment where good IAAS tools provision and start nodes automatically and dynamically in response to whatever the current environmental conditions call for.

As an example, suppose we're running our WordCount example distributed and we find that we are overloaded. That we are overloaded can be discovered in any number of ways including: examination of Dempsy monitoring points (see the section on Monitoring) or due to back pressure on the source (e.g. say the words are being drawn from a traditional message queuing system and Dempsy is configured not to discard messages when it gets behind, the queue of words would begin to lengthen indicating the need for more processing power to keep up) or by simply looking at the system metrics on the nodes (CPU and memory).

In response to any or all of these conditions new nodes brought online will automatically join the processing.

## Application Definition

As we have seen, Dempsy needs some minimal description of an application. With "Dynamic Topology" this description itself doesn't need to be centralized. In the examples we have seen to date the Application Definition described all of the MessageProcessors that participate in an application. For many, if not most applications, this is a good way to go since everything is defined in one place. 

Let's review. In the [Word Count](Simple-Example) example the application definition fully describes the application by listing which message processors participate in the application. Note, this configuration does _NOT_ describe the topology (which message processors send messages to which other message processors). Again, that information is discovered at runtime and so the order of the `ClusterDefinition`s below makes no difference.

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="word-count" />
    <property name="clusterDefinitions">
      <list>
        <bean class="com.nokia.dempsy.config.ClusterDefinition">
          <constructor-arg value="adaptor"/>
          <property name="adaptor">
            <bean class="com.nokia.dempsy.example.userguide.wordcount.WordAdaptor" />
          </property>
        </bean>
        <bean class="com.nokia.dempsy.config.ClusterDefinition">
          <constructor-arg value="mp"/>
          <property name="messageProcessorPrototype">
            <bean class="com.nokia.dempsy.example.userguide.wordcount.WordCount"/>
          </property>
        </bean>
      </list>
    </property>
  </bean>
</beans>
```

Notice, an `ApplicationDefinition` contains `ClusterDefinition`s and, remembering our [Terminology](Terminology), a <em>Cluster</em> is the set of all message processors of a particular type that are all participating in the same step of an application.

There are several parameters that can be set for both the `ApplicationDefinition` and the `ClusterDefinition`. All of the `ApplicationDefinition` parameters allow for the defaulting of <em>Cluster</em> specific settings and can be redefined or overridden at the cluster level using the `ClusterDefinition`. The `ApplicationDefinition` parameters include:

1. `serializer` - <em>Optional</em> - You can set the serialization scheme for the transferring of messages between message processors. Out of the box, Dempsy comes with two serializers that can be used here. One supports straightforward java serialization and one supports [Kryo](http://code.google.com/p/kryo/). Kryo is the default but should be tuned for production applications. See the section on [Serialization] for how to correctly tune Kryo within Dempsy. Custom serialization can also be defined. See the section on [Framework Development](Framework-Development) and specifically the section on the [Serialization Api](Serialization-Api).
1. `statsCollectorFactory` - <em>Optional</em> - This setting provides for either the complete override the monitoring back-end, or the fine tuning of the main out-of-the-box implementation. The main implementation is very powerful and so the main use of this setting will be to fine tune the monitoring parameters. For information on how to completely replace the monitoring see the section on the [Monitoring Api](Monitoring-Api) as part of [Framework Development](Framework-Development). See the section on [Monitoring Configuration](Monitoring-Configuration) for details on how to tune the Dempsy statistics collector.
1. `executor` - <em>Optional</em> - _This may change prior to a 1.0 release_. Dempsy provides for the ability to tune, or completely redefine, the default threading model for processing. To understand how to replace the threading model, see the section on the [Executor Api](Executor-Api) as part of [Framework Development](Framework-Development). To tune the existing implementation see the section on [Executor Configuration](Executor-Configuration).

Each of the above parameters can be overridden in the `ClusterDefinition`. In addition, the `ClusterDefinition` provides for the setting of the following:

1. `adaptor` - _Required either `adaptor` or `messageProcessorPrototype`_ - Setting the Adaptor defines a special type of cluster that defines the source for streaming data. The object provided must be an instance of the `Adaptor` interface. See the [Adaptors section of the Simple Example](Simple-Example#wiki-adaptors) and the api docs for the [Adaptor interface](http://dempsy.github.com/Dempsy/appdev-apidocs/com/nokia/dempsy/Adaptor.html) for more information.
1. `adaptorDaemon` - _Optional_ - this is a boolean property that identifies whether or not the thread that the Adaptor runs in should be a `daemon` thread. The default is `false`. It's really not a good idea to use this parameter as it tells Dempsy to ignore the shutting down of the Adaptor. Ideally an `adaptor` should shutdown cleanly in response to a `stop`. This property is ignored if the `ClusterDefninition` doesn't have the `adaptor` set.
1. `messageProcessorPrototype` - _Required either `adaptor` or `messageProcessorPrototype`_ - This is how a prototype for the message processor is provided to the cluster. The object provided must be annotated with the `@MessageProcessor` annotation. See the [Message Processor Prototype section of the Simple Example](Simple-Example#wiki-themessageprocessorprototypew) and the api docs for the [@MessageProcessor annotation](http://dempsy.github.com/Dempsy/appdev-apidocs/com/nokia/dempsy/annotations/MessageProcessor.html) for more information.
1. `evictionTimePeriod` and `evictionTimeUnit` - _Optional_ - When a _message processor_ is [evictable](Eviction) these properties allow for the tuning of the frequency of eviction checking. If one is set, they should both be set. The default is 600 seconds (10 minutes). The time unit is a instance of the enum `java.util.concurrent.TimeUnit`. If the _message processor_ isn't evictable then setting these properties has no effect.
1. `outputExecutor` - _Required if the message processor needs to run @Output cycles_ - This property is used to set the scheme for invoking an @Output cycle. See the section in the Simple Example on [Non-stream Driven Message Processor Output](Output) for more information. Especially the _Advanced Options_ section. This setting can be used to define new schemes following the details in the section on the [Output Executor Api](OutputExecutor-Api) as part of [Framework Development](Framework-Development).

## Serialization Configuration and Tuning

As was previously mentioned, Dempsy is built on a set of core abstractions. One of those abstractions is _Serialization_. This means the entire serialization scheme for the framework is pluggable. If you're interested in plugging in custom serialization then see the section on [Framework Development](Framework-Development) and specifically the section on the [Serialization Api](Serialization-Api).

Dempsy comes with two serializer implementations. The first is straightforward java serialization. The second is [Kryo](http://code.google.com/p/kryo/), which is the default if you don't specify it in the [Application Definition or Cluster Definition](ApplicationDefinition).

### Java Serialization

<table>
    <tr>
        <td>Note: Currently the default implementation for the Serializer is <a href="http://code.google.com/p/kryo/">Kryo</a>. Kryo, when used correctly, can  be 8 to 10 times more efficient, in terms of both network traffic and raw serialize/deserialize performance, than straight Java serialization. Please do not use Java serialization for any production systems that require efficiency unless you plan on hand optimizing all of your message serialization. <b>You have been warned.</b></td>
    </tr>
</table>

As the name implies, Java serialization is simply an implementation of the core abstraction that uses standard java serialization. Therefore, in order for it to work, all of your message classes and their attributes need to implement `java.io.Serializable`. If they don't you will get _send failures_ in Dempsy.

In order to enable java serialization you simply provide the implementation to the Application Definition (which is over-ridable in the Cluster Definition as described in the [Application Definition](ApplicationDefinition) section. An example follows:

```xml
...
<bean class="com.nokia.dempsy.config.ApplicationDefinition">
   <constructor-arg value="ApplicationName"/>
   <property name="serializer">
       <bean class="com.nokia.dempsy.serialization.java.JavaSerializer"/>
   </property>
   ...
```

### Kryo Serialization

If you don't do anything, Kryo will be used for the default serialization. However, Kryo can be made much more efficient with a little bit of configuration.

 <table>
    <tr>
        <td>Note: In order to use the defaults, every class that gets serialized must have a default constructor. Kryo will work with private default constructors as long as the security settings allow for it. For more details on Kryo's requirements for object creation, see the <a href="http://code.google.com/p/kryo/#Object_creation">Kryo documentation on object creation</a></td>
    </tr>
</table>

#### Class Registration

The easiest way to optimize Kryo is to provide the configuration with an ordered list of all of the classes that will be serialized as part of the application. The order of the list should be most-frequently serialized to least frequently serialized. <b>Note: it is critical that all senders and receivers have the same list in the same order</b> and the best way to make this happen is to make this list part of the Application Definition.

In order to list the classes you will provide the instance of Dempsy serializer in the Application Definition just like in the case of the Java Serializer except we will include additional parameters. For example, the Application Definition for the Word Count example could look like this:

```xml
<bean class="com.nokia.dempsy.config.ApplicationDefinition">
   <constructor-arg value="WordCount"/>
   <property name="serializer">
      <bean class="com.nokia.dempsy.serialization.kryo.KryoSerializer" >
         <constructor-arg>
           <list>
             <bean class="com.nokia.dempsy.serialization.kryo.Registration" >
               <constructor-arg value="com.nokia.dempsy.example.userguide.wordcount.Word" />
             </bean>
             <bean class="com.nokia.dempsy.serialization.kryo.Registration" >
               <constructor-arg value="com.nokia.dempsy.example.userguide.wordcount.CountedWord" />
             </bean>
           </list>
         </constructor-arg>
         ...
```

Note the `Word` class is listed prior to the `CountedWord` class. It's safe to say, given the nature of the Word Count application, that there will be many more `Word` messages than `CountedWord` messages and therefore, `Word` is listed first.

By default the Kryo serializer will work with unregistered classes, however, there is a way to tell the Kryo serializer that all classes must be registered. Since registered classes are so much more efficient, during development it is a good idea to set this option. Setting this options causes serialization to fail on any unregistered class. This will require the developer to register the class to resolve the problem and thereby assure that all classes are registered.

To enable this option you set the following property:

```xml
...
      <bean class="com.nokia.dempsy.serialization.kryo.KryoSerializer" >
       ...
          <property name="kryoRegistrationRequired" value="true" />
          ...
```

#### Advanced Configuration

Dempsy provides a hook into the Kryo serializer that allows for the application developer to get access to the underlying `Kryo` implementation and thereby opens up all of the underlying Kryo options to the developer. You will need to be familiar with Kryo itself in order to use it but if you want access, you need to implement the `com.nokia.dempsy.serialization.kryo.KryoOptimizer` interface.

The implementation of the `com.nokia.dempsy.serialization.kryo.KryoOptimizer` interface will be given the central `com.esotericsoftware.kryo.Kryo` instance both before any registration happens, and then again afterward. Things to keep in mind:

1. If you want to modify or add to the `com.esotericsoftware.kryo.Kryo` instance before any Registrations are applied, implement the `void preRegister(com.esotericsoftware.kryo.Kryo kryo)` method to do that. Otherwise implement it to do nothing.
1. If you want to modify or add to the `com.esotericsoftware.kryo.Kryo` instance after all of the Registrations are applied, implement the `void postRegister(com.esotericsoftware.kryo.Kryo kryo)` method to do that. Otherwise implement it to do nothing.
1. Dempsy's Kryo based serializer pools `com.esotericsoftware.kryo.Kryo` instances since they are not thread safe. They are created 'as needed' and therefore the `KryoOptimizer` will be used each time a new `com.esotericsoftware.kryo.Kryo` instance is created and pooled.

To provide your implementation of the `KryoOptimizer` you set the property on the Serializer as follows:

```xml
...
      <bean class="com.nokia.dempsy.serialization.kryo.KryoSerializer" >
       ...
          <property name="kryoOptimizer">
              <bean class="com.mycompany.myapplication.MyOptimizer" />
          </property>
      ...
```

## Monitoring

Dempsy monitoring is very flexible, detailed, powerful, and designed to be used in a widely distributed system. Out of the box it supports integration with several different "reporting back ends" including:

1. Full integration with [Gaphite](http://graphite.wikidot.com/)
1. Full integration with [Ganglia](http://ganglia.sourceforge.net/)
1. It can write all metrics to a set of CSV files
1. It can write all metrics to the console

... or any combination of the above. With very little effort you can merge the Dempsy framework metrics with application metrics and piggyback your own application monitoring points onto the same system.

Or, like many of the other core abstractions that Dempsy is built on, you can completely replace the implementation and develop your own monitoring extensions. For more information on this see the section on [Framework Development](Framework-Development) and specifically the section on the [Monitoring Api](Monitoring-Api).

Dempsy's default monitoring is built on the [Yammer Metrics library by Coda Hale](http://metrics.codahale.com/), an incredibly simple, flexible and versatile monitoring abstraction.

### Framework Monitoring Metrics

The following lists all of the framework metrics that are gathered. Depending on the back-end selected, aggregations of individual metrics can also be supplied. For example, when using Graphite you can see, not only the raw message count, but also 1-minute, 5-minute, and 10-minute average message rates, among others. Graphite will also provide very useful duration/timing aggregates including medians, standard-deviations, and various _percentile_ times (99th, 99.9th, etc) which can be really useful in tracking down application problems. These aggregations will not be listed in the following table, but can be inferred depending on the aggregation capabilities of the reporting back end.

Keep in mind, metrics are specific to a particular _cluster_ within a particular _node_. Aggregation is provided by the "reporting back end" functionality.

The metrics are broken into three sets. It's useful to keep this distinction in mind:

1. Receive side metrics are those that track information about message receiving. They deal with monitoring what happens from when a message comes into a _node_ to when it's routed to a _message processor_.
1. Processing metrics are those that track information about the processing of messages, including timing, counts and failures.
1. Send side metrics are those that track information about message sending. They deal with monitoring what happens from when a message is dispatched from an _Adaptor_ or returned from a _message processor_ to when it's sent on to the next step in the processing.

#### Receive side metrics

<table>
<tr>
<th> Metric </th><th> Description </th>
</tr>
<tr>
<td> messages-received </td><td> How many messages were successfully received by this <em>node</em>. Note that in <em>Adaptors</em> this will always be zero.</td>
</tr>
<tr>
<td> bytes-received </td><td> How many bytes were successfully received by this <em>node</em>. Note that in <em>Adaptors</em> this will always be zero.</td>
</tr>
<tr>
<td> messages-discarded</td><td> This metric indicates how many messages were discarded. This metric does <b>not</b> incorporate 'failed' messages (see the entry below) but does include 'collisions' (see the entry below). Dempsy usually discards messages due to load when queues fill up. It will always opt to discard the oldest messages. This metric will always be zero in an <em>Adaptor</em>.</td>
</tr>
<tr>
<td> messages-collisions </td><td> Depending on the transport and how it's configured, Dempsy may discard messages if one shows up for a <em>message processor</em> while it's busy working on something else. Transport defaults will queue messages when collisions happen rather than discard them so this metric will always be zero unless the transport configuration is told to 'fail fast'. See the details on the various Transports for more information. This metric is also incorporated into the 'messages-discarded' described above. This metric will always be zero in an <em>Adaptor</em>.</td>
</tr>
<tr>
<td> messages-pending </td><td> The total number of queued messages waiting to be processed at any given point in time. This metric is dependent on the underlying transport and isn't supported in all cases. If the transport queues incoming messages, then it should be supplied. This metric will always be zero in an <em>Adaptor</em>.</td>
</tr>
</table> 

#### Processing Metrics

<table>
<tr>
<th> Metric </th><th> Description </th>
</tr>
<tr>
<td> messages-dispatched </td><td> This is the total number of messages that have been dispatched to a <em>message processor</em> no matter what the eventual disposition of the processing is. This metric will always be zero in an <em>Adaptor</em>.</td>
</tr>
<tr>
<td> messages-processed</td><td> This metric indicates how many messages were successfully processed by a _message processor_. These include all messages provided to a <em>message processor</em> where the<em>message processor</em>'s @MessageHandler returned without throwing an exception, whether or not it returned a message to be forwarded on. This metric will always be zero in an <em>Adaptor</em>.</td>
</tr>
<tr>
<td> messages-mp-failed </td><td> This metric includes all messages provided to a <em>message processor</em> where the <em>message processor</em>'s @MessageHandler threw and exception. This metric will always be zero in an <em>Adaptor</em>. </td>
</tr>
<tr>
<td> messages-dempsy-failed </td><td> This metric includes all message that failed due to a framework/container error when a message was presented to a <em>message processor</em>. These are extremely rare and will be accompanied by an error in the log detailing the problem. This metric will always be zero in an <em>Adaptor</em>. </td>
</tr>
<tr>
<td> message-processors-created </td><td> The total number of <em>message processors</em> that have been created. This is <b>not</b> the total number of message processor currently in the container but the number that have been created since the <em>container</em> was started. Therefore the number is always increasing even when eviction is enabled or elasticity moves instances. The current total number of live <em>message processor</em> will be 'message-processors-created' - 'message-processors-deleted' metrics (see below for 'message-processors-deleted'). This metric will always be zero in an <em>Adaptor</em>.</td>
</tr>
<tr>
<td> message-processors-deleted </td><td> The total number of <em>message processors</em> that have been @Passivated. There are several reasons that a <em>message processor</em> might be passivated. Please review the section on <a href="Activation#wiki-passivation">Passivation</a> for more information. This metric will always be zero in an <em>Adaptor</em>.
</tr>
<tr>
<td> message-processors </td><td> A snapshot of the total number of <em>message processors</em> currently active in the container. This metric will always be zero in an <em>Adaptor</em>.
</tr>
<tr>
<td> mp-handle-message-duration </td><td> The duration of calls on the <em>message processor's</em> @MessageHandler method. Aggregates of this value can be useful in determining application performance bottlenecks. This metric will always be zero in an <em>Adaptor</em>.
</tr>
<tr>
<td> outputInvoke-duration </td><td> The duration of <a href="Output">@Output</a> passes. See the section on <a href="Output">Non-stream Driven Message Processor Output</a>. Aggregates of this value can be useful in determining application performance bottlenecks in the output processing of the application. This metric will always be zero when the <em>message processor</em> has no @Output method or in an <em>Adaptor</em>.
</tr>
<tr>
<td> evictionInvoke-duration </td><td> The duration of <a href="Eviction">Eviction check</a> passes. See the section on <a href="Eviction">Eviction</a>. This metric will always be zero when the <em>message processor</em> has no @Evictable method or in an <em>Adaptor</em>.
</tr>
<tr>
<td> pre-instantiation-duration </td><td> The duration of the <a href="Pre+instantiation">pre-instantiation</a> pass. This metric will always be zero in an <em>Adaptor</em>.
</tr>
</table> 

#### Send side metrics

<table>
<tr>
<th> Metric </th><th> Description </th>
</tr>
<tr>
<td> messages-sent </td><td> How many messages were successfully sent out of this <em>node</em>. </td>
</tr>
<tr>
<td> bytes-sent </td><td> How many bytes were successfully sent out of this <em>node</em>.</td>
</tr>
<tr>
<td> messages-unsent </td><td> This metric indicates how many messages where an attempt was made to send, but were discarded prior to successfully transmitting the message. This count is <b>not</b> incorporated in the above  'messages-sent' metric since that only covers successfully transmitted messages. There are several reasons this metric can be triggered. They include: 1) There is no current destination available for the message. 2) The message has no @MessageKey or reflection failed to retrieve it. 3) A shutdown happens while the message is being queued. 4) An exception (likely an I/O exception) happens when attempting to transmit the message. 4) There are too many messages queued up to be sent out to a particular destination. In this case Dempsy will always opt to discard the oldest messages.</td>
</tr>
<tr>
<td> messages-out-pending </td><td> The total number of queued messages waiting to be transmitted at any given point in time. This metric is dependent on the underlying transport and isn't supported in all cases. If the transport queues outgoing messages, then it should be supplied.</td>
</tr>
</table> 

### Monitoring Configuration

The monitoring can be tuned in several different ways but each of these require setting the `statsCollectorFactory` property on the [Application Definition](ApplicationDefinition) and setting parameters on the implementation provided. The property is set to an implementation of the interface `com.nokia.dempsy.monitoring.StatsCollectorFactory` (again, see the [Monitoring Api](Monitoring-Api) section for more details on this interface and how to provide your own implementation). The implementation provided with Dempsy is called `com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda` (named after [Coda Hale](http://codahale.com/)).

As an example, one that will be the basis of the following sections, you would add the following to your Application Definition for your application:

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="myApplication" />
    ...        
    <property name="statsCollectorFactory">
        <bean class="com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda" />
    </property>
    ...
```

Of course, without any properties set on the `StatsCollectorFactoryCoda` there's nothing more than the default already gives you. The properties that can be set are described in the following sections: 

#### Enabling various monitoring reporting back-ends

By default, Dempsy's monitoring implementation isn't configured with any "reporting back-ends" (see the enumerated list above). However, <b>all metrics are automatically exposed via JMX even if no (other) reporting back ends are configured</b>. If you want to use one of the reporting back-ends you need to supply a list of reporting specs (`com.nokia.dempsy.monitoring.coda.MetricsReporterSpec` instances) to the configuration of the `StatsCollectorFactoryCoda` implementation.

To select different reporting back ends you use the `type` property on the `MetricsReporterSpec`. The following shows how to select Graphite:

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="myApplication" />
    ...        
    <property name="statsCollectorFactory">
      <bean class="com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda" >
        <property name="reporters">
          <list>
            <bean class="com.nokia.dempsy.monitoring.coda.MetricsReporterSpec">
              <property name="type">
                <value type="com.nokia.dempsy.monitoring.coda.MetricsReporterType">GRAPHITE</value>
              </property>
    ...
```

`MetricsReporterType` is an enum with the values: `GRAPHITE`,`GANGLIA`,`CONSOLE`,`CSV`.

<table><tr><td> Note: You can enable multiple reporting back ends simultaneously by providing multiple MetricsReporterSpecs in the list for the "reporters" property.</td></tr></table>

<table><tr><td> Note: Keep in mind, all metrics are automatically reported through JMX not matter which, or how many, "reporting back-ends" are enabled.</td></tr></table>

##### Configuring Graphite

Here is an example of how you configure Graphite as the reporting back end for the monitoring:

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="myApplication" />
    ...        
    <property name="statsCollectorFactory">
      <bean class="com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda" >
        <property name="reporters">
          <list>
            <bean class="com.nokia.dempsy.monitoring.coda.MetricsReporterSpec">
              <property name="type">
                <value type="com.nokia.dempsy.monitoring.coda.MetricsReporterType">GRAPHITE</value>
              </property>
              <property name="period" value="1"/>
              <property name="unit">
                <value type="java.util.concurrent.TimeUnit">MINUTES</value>
               </property>
               <property name="hostName" value="${graphite.host}"/>
               <property name="portNumber" value="${graphite.port}"/>
            </bean>
          </list>
        </property>
      </bean>
    </property>
    ...
```

When configuring the `StatsCollectorFactoryCoda` to report to Graphite you <b>MUST</b> supply:

1. the: `period` and the `unit`, together these provide the time period for successive outputs to Graphite
1. the `hostName` and `portNumber` for where the Graphite server is listening.

Notice that in the above example, the Spring configuration assumes the `PropertyPlaceholderConfigurer` or `-D` command line parameters are being used to supply the graphite host name and port.

##### Configuring Ganglia

Ganglia configuration is identical to Graphite configuration except the `type` is `GANGLIA`. For example:

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="myApplication" />
    ...        
    <property name="statsCollectorFactory">
      <bean class="com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda" >
        <property name="reporters">
          <list>
            <bean class="com.nokia.dempsy.monitoring.coda.MetricsReporterSpec">
              <property name="type">
                <value type="com.nokia.dempsy.monitoring.coda.MetricsReporterType">GANGLIA</value>
              </property>
              <property name="period" value="1"/>
              <property name="unit">
                <value type="java.util.concurrent.TimeUnit">MINUTES</value>
               </property>
               <property name="hostName" value="${ganglia.host}"/>
               <property name="portNumber" value="${ganglia.port}"/>
            </bean>
          </list>
        </property>
      </bean>
    </property>
    ...
```

Again, like for Graphite, you <b>MUST</b> supply:

1. the: `period` and the `unit`, together these provide the time period for successive outputs to Ganglia
1. the `hostName` and `portNumber` for where the Ganglia server (carbon-cache agent) is listening.

##### Configuring CSV output

For CSV output you just need to supply the output directory. The CSV reporter will periodically append data to metric-specific files in that output directory. An example of how to configure it follows:

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="myApplication" />
    ...        
    <property name="statsCollectorFactory">
      <bean class="com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda" >
        <property name="reporters">
          <list>
            <bean class="com.nokia.dempsy.monitoring.coda.MetricsReporterSpec">
              <property name="type">
                <value type="com.nokia.dempsy.monitoring.coda.MetricsReporterType">CSV</value>
              </property>
              <property name="period" value="1"/>
              <property name="unit">
                <value type="java.util.concurrent.TimeUnit">MINUTES</value>
               </property>
               <property name="outputDir" >
                 <bean class="java.io.File">
                    <constructor-arg value="${csv.outdir}" />
                 </bean>
               </property>
            </bean>
          </list>
        </property>
      </bean>
    </property>
    ...
```

You <b>MUST</b> supply:

1. the: `period` and the `unit`, together these provide the time period for successive outputs to the individual CSV files.
1. the `outputDir` of where to write the files.

##### Configuring Console output

You can configure Dempsy to periodically report the metrics to the console (stdout):

```xml
<beans>
  <bean class="com.nokia.dempsy.config.ApplicationDefinition">
    <constructor-arg value="myApplication" />
    ...        
    <property name="statsCollectorFactory">
      <bean class="com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda" >
        <property name="reporters">
          <list>
            <bean class="com.nokia.dempsy.monitoring.coda.MetricsReporterSpec">
              <property name="type">
                <value type="com.nokia.dempsy.monitoring.coda.MetricsReporterType">CONSOLE</value>
              </property>
              <property name="period" value="1"/>
              <property name="unit">
                <value type="java.util.concurrent.TimeUnit">MINUTES</value>
               </property>
            </bean>
          </list>
        </property>
      </bean>
    </property>
    ...
```

You <b>MUST</b> supply the: `period` and the `unit`, together these provide the time period for successive outputs to the console.

#### Metric Naming

Metrics are named hierarchically and various reporting back-ends (and JMX) handle these names differently. Metric naming is important for management and aggregation purposes. For example, it helps to be able to separate the 'messages-received' coming from different applications, hosts, or difference deployment environments (development, testing, etc). This is especially important where the metrics are centrally gathered, tracked and managed.

By default, Dempsy provides it's metrics using the following naming scheme:

```
[environment prefix][node name][application name]-[cluster name].Dempsy.[metric name]
```

Note that the _environment prefix_ and the _node_ only apply to the Graphite and Ganglia reporting back-ends and do not effect the names of the monitoring points in either the other back-ends or in JMX.

Keep in mind that some back-ends, like Graphite, give special meaning to the '.' character.

The _environment prefix_ provides a means to prepend an indication as to which environment this node is running in. This allows for the use of a single Graphite or Ganglia server to monitoring multiple environments and yet, at the top of the hierarchy, separate all of the metrics by which environment they're from. 

You supply the environment prefix in one of two ways. First, you can set the `environmentPrefix` property directly on the `StatsCollectorFactoryCoda` instance when configuring it as described above. Alternatively you can provide a system property using `-Denvironment.prefix=...`. The system property will take precedence.

The _environment prefix_ doesn't assume a '.' after it by default so if you want the back-end to acknowledge the environment as a place in the hierarchy of the name, then you should include the '.' when you specify the prefix. For example, in Graphite, there's a difference between `-Denvironment.prefix=test-` and `-Denvironment.prefix=test.`. The latter will allow the collapsing of the hierarchy under the top level environment name, the former will not. This allows the person providing the environment name to determine whether or not they want it flat, or hierarchical.

The _environment prefix_ has no effect on the name within JMX or within reporting back-ends other than Graphite and Ganglia.

The _node name_ is derived from the transport. In most cases it will be the IP Address of the host that the node in question is running on. However, since the IP address has '.' characters in it, this would cause Graphite to create a hierarchy out of the octets of the address so the '.' characters are replaced with '-' characters.

The _node name_ has no effect on the name within JMX or within reporting back-ends other than Graphite and Ganglia.

Next comes the `[application name]-[cluster name]`. This is directly from the Application Definition and Cluster Definition that's driving the _node_ in question. The preference here is to flatten out the metrics (not use the '.'). This is a preference based on experience. If you really don't like this you can overload the naming scheme following the directions in the [Monitoring Api](Monitoring-Api).

In order to separate framework metrics from application metrics, a point in the hierarchy is inserted using `.Dempsy.` which will cause all of the framework metrics to be grouped under a `Dempsy` subset.

Each metric from the tables above will then be the last part of the name. Below this point in the name hierarchy will be the specific aggregates provided by [Yammer Metrics](http://metrics.codahale.com/) and will depend on the type of metric.

#### Providing Application Specific Metrics

Since Dempsy is built on [Yammer Metrics](http://metrics.codahale.com/), then simply using Yammer Metrics directly in your application will expose those metrics through whatever back-ends are configured along with the framework metrics.

Of course, it will help to use a naming scheme within Yammer Metrics that cooperates with the existing Dempsy framework use in order to have all of your metrics organized together with the framework metrics. There is no need to handle the _environment name_ or _node name_ portions of the metric names as those are specific to the reporting back-end and configured through the above configuration techniques. An example follows:

```java
import com.yammer.metrics.core.MetricName;

  ...
     MetricName appMetricName = 
          new MetricName(applicationName + "-" + clusterName,"application", "my-custom-application-metric");
  ...
```

If you want access to the `ClusterId` that you're particular _message processor_ is running in, you can use the [@Start method](The-Message-Processor-Lifecycle#wiki-start) on the _message processor prototype_.


## Executor Configuration

Dempsy allows fine tuning of the number of threads that are used for processing. These threads apply to all processing taking place inside the _message processor_ including message handling and [output processing](Output).

<table border="1"><tr><td>
Note: In 0.7.2 the <a href="Output">output processing</a> has it's own executor with it's own threads. The intention is to eventually use the same <em>DempsyExecutor</em> instance for both. The documentation here assumes that.
</td></tr></table>

The default is based on the number of cores available on the machine. Often the default will not be appropriate. If the MP is I/O intensive, or the message sizes are large and therefore create an I/O bottleneck in the framework, it could be advantageous to explicitly set or tune the thread pool size.

By default the thread pool is set using the following formula:

```
number of threads = m * number of cores + additional threads
```

`m` defaults to 1.25 and `additional threads` defaults to 2.

### Configuring the Thread Pool

You can set the number of threads through one of two alternative methods. The _relative_ (to the number of cores) method is by setting `m` and `additional threads`. The _absolute_ method is to simply supply the `number of threads` directly.

#### Relative configuration

The setting can be configured at either the `ApplictionDefinition` level, or the `ClusterDefinition` level. When set at the `ApplicationDefinition` level it applies to all `ClusterDefinitions` in the application.

```xml
        <property name="executor">
            <bean class="com.nokia.dempsy.executor.DefaultDempsyExecutor">
                <property name="additionalThreads" value="45"/>
                <property name="coresFactor" value="2.0"/>
            </bean>
        </property>
```

The above results in setting the number of threads to `2 * number of cores + 45` so if, for example, the number of cores is 8, the total number of threads will be 71.

#### Absolute Configuration

Alternatively you can simply set the number of threads on the executor directly. Again, the setting can be configured at either the `ApplictionDefinition` level, or the `ClusterDefinition` level. When set at the `ApplicationDefinition` level it applies to all `ClusterDefinitions` in the application.

In the following example we set the number of threads to 45:

```xml
        <property name="executor">
            <bean class="com.nokia.dempsy.executor.DefaultDempsyExecutor">
                <constructor-arg value="45"/>
                <constructor-arg value="-1"/>
            </bean>
        </property>
```

<table border="1"><tr><td>
Note: the second constructor argument is the <em>maxNumberOfQueuedLimitedTasks</em> discussed below.
</td></tr></table>

### Message Queue Limits

The `Executor` is also the place where processing queue limits are handled. For example, if the _node_ is getting behind and there are too many tasks queued up to execute, then the Executor will handle the shedding of messages.

When messages come into a _node_ they are queued for delivery to the appropriate _message processors_ but by default there are limits set. When the limits are exceeded the oldest queued messages are discarded, and all of the appropriate monitoring bookeeping is done.

By default the limit is derived from the number of threads and is currently set to `20 * thread pool size`. This can be explicitly set using either:

1. The second constructor parameter
1. The `maxNumberOfQueuedLimitedTasks` property on the `DefaultDempsyExecutor`.

If this value is set to '< 0' (it's set to `-1` in the above constructor arg. example) then the formula will be used to calculate the value.

### Blocking and Unbounded Queueing

It may be advantageous in certain testing situations to force the _message processor_ message handling to either block on message submission, or set the limiting queue to unbounded. __It is not recommended that either option be used in a production scenario for what should be obvious reasons.__

If blocking is set to true then submitting message processing tasks, once the `maxNumWaitingLimitedTasks` is reached, will block until room is available. This will create _back pressure_ on the transport.

To set blocking use:

```xml
                <property name="blocking" value="true"/>
```

Setting `unlimited` to true allows for an _unbounded queue_ to handle message processing. This instructs the  `DefaultDempsyExecutor` to effectively ignore the `maxNumWaitingLimitedTasks` setting. The default behavior is for the oldest message processing tasks to be rejected when the `maxNumWaitingLimitedTasks` is reached.

Setting `unlimited` effectively causes the Executor to ignore the `blocking` setting.

To set the `DefaultDempsyExecutor` to `unlimited` use:

```xml
                <property name="unlimited" value="true"/>
```

#Message Routing and Microsharding

As described in the [Simple Example](Simple-Example#wiki-messageprocessoraddressing), Dempsy associates an each unique _@MessageKey_ value with an individual _message processor_ instance. The main job of Dempsy then is to find the correct _message processor_ to deliver each message to, among a cluster of machines. By default, Dempsy does this through the concept of _microsharding_.

Conceptually _microsharding_ is elegant and provides several advantages over other solutions.

* It applies a _constant_ amount of computational resources to the message routing problem, independent of the number of potential or actual _message processors_ (the _message key space_ size) or compute nodes. This is as opposed to tracking the location of each individual _message processor_.
* It provides a uniform distribution of _message processors_ across compute nodes. This is as opposed to algorithms like the "distributed hash table."
* It allows for "stickiness" of _message processor_ node locations when the cluster topology changes. This is as opposed to stateless algorithms like a simple mod operation on the key's hash code over the number of nodes. In this case existing message processors would be reshuffled among the nodes every time a new node entered or left the cluster.

_Microsharding_ is a technique where the _key-space_ for all potential _message processors_ is broken up among a fixed number of _shards_. _Shards_ are assigned uniquely to the currently available compute _nodes_.  The number of _shards_ `M` is `>>` the number of _nodes_ `N`. A shard is uniquely assigned to an individual _node_ while a _node_ will have many _shards_. The relationship between which _nodes_ own which _shards_ is managed by ZooKeeper.

<div align="center">
<table align="center" width="70%" border="1" >
<tr><td>
<img width="100%" src="https://raw.github.com/wiki/Dempsy/Dempsy/images/Dempsy-microsharding.png" alt="Microsharding" />
</td>
</tr>
<tr><td><center>Microsharding</center></td></tr>
</table>
</div>

In order to locate the correct _node_ that a _message processor_ is on, Dempsy will:

1. Get the _@MessageKey_ from the _message_ instance.
1. Get the `hashCode` of the _@MessageKey_
1. Find the _shard_ associated to the _message processor_ to which the message is addressed using `shard = message key's hash code % M`
1. Route the message to the _node_ that owns the computed _shard_.

Using _microsharding_ it's easy to manage changes to the _cluster_ of _nodes_. Suppose a new _node_ enters the _cluster_. An imbalance is introduced where the _shards_ are no longer evenly distributed among the _nodes_. In this case, using ZooKeeper as the intermediary, the new _node_ will negotiate for shards until the number of shards is evenly distributed again.

As a concrete example, let's say there's 5 nodes and 100 shards. That means each _node_ has 20 _shards_. If a 6th _node_ enters the _cluster_ then the most any _node_ should have would be 17 and the least any should have would be 16. The new _node_ has zero and must acquire at least 16. The other _nodes_ all have 20 and need to give up at least 4. In Dempsy's default _RoutingStrategy_ this is done in a completely decentralized manner using ZooKeeper.

Notice that when the cluster changes, the fewest number of _message processors_ actually migrate and most of them never move.

If one of the _nodes_ then leaves the cluster we're back down to 5. When this happens the remaining _nodes_ negotiate through ZooKeeper for to acquire the additional shards. None of the _message processors_ from the _nodes_ that stayed in the cluster needed to move.

