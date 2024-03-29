# The Dempsy Project

## Table of Contents
- [The Dempsy Project](#the-dempsy-project)
  - [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Jumping right in.](#jumping-right-in)
  - [An example - The ubiquitous "Word Count"](#an-example---the-ubiquitous-word-count)
    - [The _Adaptor_](#the-adaptor)
    - [The Message](#the-message)
    - [The Message Processor (_Mp_)](#the-message-processor-mp)
    - [Running the example.](#running-the-example)
    - [Explanation.](#explanation)
  - [Running the example distributed](#running-the-example-distributed)
    - [Distributed Infrastructure Selection](#distributed-infrastructure-selection)
- [Some terminology](#some-terminology)

# Overview

Simply put _Dempsy_ (Distributed Elastic Message Processing SYstem) is an framework for easily writing distributed and dynamically scalable applications that process unbounded streams of (near-)real-time messages. Conceptually it's similar to [Apache Flink](https://flink.apache.org/) and [Apache Storm](https://storm.apache.org/index.html).

*Note: Dempsy does NOT guarantee message delivery and will opt to discard messages in the presence of "back-pressure." This means it's not suitable for all streaming applications.* However, if your application doesn't require guaranteed delivery, then _Dempsy_ provides programming model that makes distributed stream processing applications easier to develop and maintain than other frameworks.

# Jumping right in. 

## An example - The ubiquitous "Word Count"

In this example we have an stream of _Word_ messages and we want to keep track of how many times each _Word_ appears in the stream. 

You can find the complete working example here: [Simple WordCount](https://github.com/Dempsy/dempsy-examples/tree/master/simple-wordcount/src/main/java/net/dempsy/example/simplewordcount)

### The _Adaptor_

To start with we need a source of _Word_ messages. This is done in _Dempsy_ by implementing an _Adaptor_.

```java
...
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;

public class WordAdaptor implements Adaptor {
    private Dispatcher dempsy;
    private final AtomicBoolean running = new AtomicBoolean(false);

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
        while(running.get()) {
            // obtain data from an external source
            final String wordString = getNextWordFromSoucre();
            if(wordString == null) // the first null ends the stream.
                running.set(false);
            else {
                // Create a "Word" message and send it into the processing stream.
                try {
                    dempsy.dispatchAnnotated(new Word(wordString));
                } catch(IllegalAccessException | IllegalArgumentException | InvocationTargetException | InterruptedException e) {
                    throw new RuntimeException(e); // This will stop the flow of Words from this adaptor.
                                                   // Optimally you'd like to recover and keep going.
                }
            }
        }
    }

    @Override
    public void stop() {
        running.set(false);
    }

    private static final String[] wordSource = {"it","was","the","best","of","times","it","was","the","worst","of","times"};
    private int wordSourceIndex = 0;

    private String getNextWordFromSoucre() {
        if(wordSourceIndex >= wordSource.length)
            return null;
        return wordSource[wordSourceIndex++];
    }
}
```

When a `WordAdaptor` is registered with Dempsy, the following will happen in order:

1. _Dempsy_ will call `setDispatcher` and pass a `Dispatcher` that the `Adaptor` can use to _dispatch_ messages.
2. _Dempsy_ will then call the `start()` method to indicate that the `Adaptor` can start sending messages. This will be called in a separate thread so the `Adaptor` doesn't have to return from the `start()` method until it's done sending messages. However, the `Adaptor` is free to use the `Dispatcher` in its own threads if it wants and can return from `start()` without causing a problem.
3. When _Dempsy_ is shut down, the `Adaptor` will be notified by calling the `stop()` method.

### The Message

In the above the adaptor sends `Word` messages. Messages in _Dempsy_ need to satisfy a few requirements. 

1. They need to have a _MessageKey_ which uniquely identifies a _MessageProcessor_ that will handle processing that message.
2. The _MessageKey_ needs to have the appropriate identity semantics (_hashCode_ and _equals_)
3. In most cases when _Dempsy_ is distributed, the _Message_ needs to be serializable according to whatever serialization technique is chosen.

So the `Word` message can be defined as follows:

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

Using annotations you can identify the class as a _Message_. The _MessageType_ annotation tells _Dempsy_ that the full class name identifies a _Dempsy compliant message_. Notice it satisfies the criteria:

1. It has a _MessageKey_ which can be retrieved by calling `getWordText()`. 
2. The _MessageKey_ is a `String` which has appropriate identity semantics.
3. The `Word` class is serializable when using _Java_ serialization.

### The Message Processor (_Mp_)

Dempsy will route each message to an appropriate _Message Processor_. A unique _Message Processor_ instance will handle each `Word` message with a given _MessageKey_. For example:

```java
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.Mp;

@Mp
public class WordCount implements Cloneable {
    private long count = 0;

    @MessageHandler
    public void countWord(final Word word) {
        count++;
        System.out.println("The word \"" + word.getWordText() + "\" has a count of " + count);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
```

So when Dempsy receives a message of type `Word`, it retrieves the _MessageKey_ using the annotated method `getWordText()`. That _MessageKey_ will become the address of a _message processor_ somewhere on the system. Dempsy will find the _message processor_ instance (in this case an instance of the class `WordCount`) within a _cluster_ of _nodes_ responsible for running the `WordCount` message processing. In the case that the instance doesn't already exist, Dempsy will `clone()` a `WordCount` instance prototype.


<div align="center">
<table align="center" border="2" >
<tr><td><center>Note: You should consider the <i>MessageKey</i> as the address of a unique <i>MessageProcessor</i> instance.</center></td></tr>
</table>
</div>

Dempsy will manage the lifecycle of _Message Processor_ instances. It will start with a single instance that will be used as a _Prototype_. When it needs more instances it will `clone()` the prototype. In this example _Dempsy_ will create an instance of `WordCount` for every unique _MessageKey_ of a `Word` message that gets dispatched. It will call the _MessageHandler_ on the corresponding instance.

### Running the example.

The following will pull all the pieces together and process a group of `Word`s.

```java
...

import net.dempsy.NodeManager;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.Node;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.monitoring.dummy.DummyNodeStatsCollector;
import net.dempsy.transport.blockingqueue.BlockingQueueReceiver;

public class SimpleWordCount {

    public static void main(final String[] args) {

        @SuppressWarnings("resource")
        final NodeManager nodeManager = new NodeManager()
            // add a node
            .node(
                // a node in an application called word-count
                new Node.Builder("word-count")
                    // with the following clusters
                    .clusters(
                        // a cluster that has the adaptor
                        new Cluster("adaptor")
                            .adaptor(new WordAdaptor()),
                        // and a cluster that contains the WordCount message processor
                        new Cluster("counter")
                            .mp(new MessageProcessor<WordCount>(new WordCount()))
                            // with the following routing strategy
                            .routingStrategyId("net.dempsy.router.simple")

                    )
                    // this will basically disable monitoring for the example
                    .nodeStatsCollector(new DummyNodeStatsCollector())
                    // use a blocking queue as the transport mechanism since this is all running in the same process
                    .receiver(new BlockingQueueReceiver(new ArrayBlockingQueue<>(100000)))
                    .build()

            )

            // define the infrastructure to be used. Since we're in a single process
            // we're going to use a local collaborator. Alternatively we'd specify
            // using zookeeper to coordinate across processes and machines.
            .collaborator(new LocalClusterSessionFactory().createSession());

        // start dempsy processing for this node in the background.
        nodeManager.start();

        // wait for the processing to be complete
        ...
        nodeManager.stop();

        System.out.println("Exiting Main");
    }
}
```

The output from running the example is:

```bash
The word "it" has a count of 1
The word "worst" has a count of 1
The word "was" has a count of 1
The word "times" has a count of 1
The word "the" has a count of 1
The word "of" has a count of 1
The word "best" has a count of 1
The word "it" has a count of 2
The word "the" has a count of 2
The word "times" has a count of 2
The word "was" has a count of 2
The word "of" has a count of 2
Exiting Main
```

### Explanation.

In this example we have a _Dempsy_ application with a single _node_ with two _clusters_. One _cluster_ contains the `WordAdaptor` and another contains the set of `WordCount` instances being used as _message processors_.

Once the example runs to completion, the number of `WordCount` _message processors_ will be equal to the number of unique _message keys_ from all of the _messages_ streamed. In this case the number is:

```java
   Set.of("it","was","the","best","of","times","it","was","the","worst","of","times").size()
```

So there will be 7 instances of `WordCount` being used as _message processors_ and an additional one representing the _message processor prototype_.

This is illustrated in the following:

<div align="center">
<table align="center" width="70%" border="1" >
<tr><td>
<img width="100%" src="https://github.com/Dempsy/dempsy/raw/master/doc/images/SimpleExample.gif" alt="WordCount pipline so far" />
</td>
</tr>
<tr><td><center>Fig. 1 Simple WordCount example</center></td></tr>
</table>
</div>


## Running the example distributed

To run the ["Word Count" example](#an-example---the-ubiquitous-word-count) distributed we need to change some of the infrastructure we instantiated. But first, lets convert the stream of words to an _unbounded_ stream by looping in the `WordAdaptor`. We'll simply change `WordAdaptor.getNextWordFromSoucre()` to the following:

```java
    private String getNextWordFromSoucre() {
        if(wordSourceIndex >= wordSource.length)
            wordSourceIndex = 0;
        return wordSource[wordSourceIndex++];
    }
```

### Distributed Infrastructure Selection

To change the infrastructure we need start _Dempsy_ selecting distributed implementations. The updated `SimpleWordCount` class would be:

```java
...
import net.dempsy.NodeManager;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.zookeeper.ZookeeperSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.Node;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.monitoring.dummy.DummyNodeStatsCollector;
import net.dempsy.serialization.jackson.JsonSerializer;
import net.dempsy.serialization.java.JavaSerializer;
import net.dempsy.transport.tcp.nio.NioReceiver;

public class SimpleWordCount {

    public static void main(final String[] args) throws InterruptedException, IllegalStateException, IllegalArgumentException, ClusterInfoException {

        final WordAdaptor wordAdaptor = new WordAdaptor();

        @SuppressWarnings("resource")
        final NodeManager nodeManager = new NodeManager()
            // add a node
            .node(
                // a node in an application called word-count
                new Node.Builder("word-count")
                    // with the following clusters
                    .clusters(
                        // a cluster that has the adaptor
                        new Cluster("adaptor")
                            .adaptor(wordAdaptor),
                        // and a cluster that contains the WordCount message processor
                        new Cluster("counter")
                            .mp(new MessageProcessor<WordCount>(new WordCount()))
                            // with the following routing strategy
                            .routingStrategyId("net.dempsy.router.managed")

                    )
                    // this will basically disable monitoring for the example
                    .nodeStatsCollector(new DummyNodeStatsCollector())
                    // use a Java NIO the transport mechanism
                    .receiver(new NioReceiver<Object>(new JavaSerializer()))
                    .build()

            )

            // define the infrastructure to be used.

            // we want to connect to a zookeeper instance running on this machine.
            .collaborator(new ZookeeperSessionFactory("localhost:2181", 3000, new JsonSerializer()).createSession());

        // start dempsy processing for this node in the background.
        nodeManager.start();

        // wait for the node manager to be started.
        while(!nodeManager.isReady())
            Thread.yield();

        // we're just going to wait *forever*
        Thread.sleep(999999);
    }
}
```

The changes from the original example include:

1. the routing strategy is now set using: `.routingStrategyId("net.dempsy.router.managed")`. The "managed" routing strategy attempts to dynamically distribute all _message processors_ for a given cluster (in this case, all `WordCount` instances) across all available _nodes_.
2. the receiver is set using: `.receiver(new NioReceiver<Object>(new JavaSerializer()))`. This identifies the technique that this node can be reached as using _Java NIO_ with the given serialization technique.
3. the dempsy nodes will collaborate with each other using [Apache Zookeeper](https://zookeeper.apache.org/) as set using: `.collaborator(new ZookeeperSessionFactory("localhost:2181", 3000, new JsonSerializer()).createSession())`. 

Now if we start multiple instances of this Java program, and we have [Apache Zookeeper](https://zookeeper.apache.org/) running on port 2181, then the instances of `WordCount` _message processors_ will be balanced between the running nodes.

If you want to try it you can start a single node instance of [Apache Zookeeper](https://zookeeper.apache.org/) using docker with the following command:

```bash
docker run --name zookeeper --network=host -d zookeeper
```

Then you can run as many instances of `SimpleWordCount` as you like.

A working version of this example can be found here: [Distributed Simple Word Count Example](https://github.com/Dempsy/dempsy-examples/tree/master/distributed-simple-wordcount/src/main/java/net/dempsy/example/simplewordcount)

<div align="center">
<table align="center" width="70%" border="1" >
<tr><td>
<img width="100%" src="https://github.com/Dempsy/dempsy/raw/master/doc/images/DistributedWordCountExample.png" alt="WordCount pipline so far" />
</td>
</tr>
<tr><td><center>Fig. 2 Distributed Simple WordCount example</center></td></tr>
</table>
</div>

# Some terminology

Having gone through the  ["Word Count" example](#an-example---the-ubiquitous-word-count) we should codify some of the terminology and concepts touched on.

<table>
<tr><th>Term</th><th>Definition</th>
</tr>
<tr>
<td> message processor </td><td> an instance of a cloned <em>message processor prototype</em> responsible for processing every <em>message</em> of a particular type with a particular unique <em>key</em>. </td>
</tr>
<tr>
<td> message processor prototype </td><td> an instance used by Dempsy to serve as a template when it needs to `clone()` more instances of a <em>message processor</em>. </td>
</tr>
<tr>
<td> message </td><td> is an object that Dempsy routes to a <em>message processor</em> based on the <em>message</em>'s <em>key</em>. </td>
</tr>
<tr>
<td> message key </td><td> A key for the message, obtained from a <em>message</em>. Each unique <em>message key</em> addresses a unique <em>message processor</em> instance in a <em>cluster</em> </td>
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
