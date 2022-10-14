# The Dempsy Project

## Table of Contents
- [The Dempsy Project](#the-dempsy-project)
  - [Table of Contents](#table-of-contents)
- [Overview](#overview)
  - [Jumping right in. An example - The ubiquitous "Word Count"](#jumping-right-in-an-example---the-ubiquitous-word-count)
    - [The _Adaptor_](#the-adaptor)
    - [The Message](#the-message)
    - [The Message Processor (_Mp_)](#the-message-processor-mp)
    - [Running the example.](#running-the-example)

# Overview

Simply put _Dempsy_ (Distributed Elastic Message Processing SYstem) is an framework for easily writing distributed and dynamically scalable applications that process unbounded streams of (near-)real-time messages. Conceptually it's similar to [Apache Flink](https://flink.apache.org/) and [Apache Storm](https://storm.apache.org/index.html).

*Note: Dempsy does NOT guarantee message delivery and will opt to discard messages in the presence of "back-pressure." This means it's not suitable for all streaming applications.* However, if your application doesn't require guaranteed delivery, then _Dempsy_ provides programming model that makes distributed stream processing applications easier to develop and maintain than other frameworks.

## Jumping right in. An example - The ubiquitous "Word Count"

In this example we have an stream of _Word_ messages and we want to keep track of how many times each _Word_ appears in the stream. 

You can find the complete working example here: [Simple WordCount](https://github.com/Dempsy/dempsy-examples/tree/master/simple-wordcount)

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

When a `WordAdaptor` is registered with Dempsy, it will do the following in order:

1. _Dempsy_ will call `setDispatcher` and pass a `Dispatcher` that the `Adaptor` can use to _dispatch_ messages.
2. _Dempsy_ will then call the `start()` method to indicate that the `Adaptor` can starting sending messages.
3. When _Dempsy_ is shut down, the `Adaptor` will be notified by calling the `stop()` method.

### The Message

In the above the adaptor sends `Word` messages. Messages in _Dempsy_ need to satisfy a few requirements. 

1. They need to have a _MessageKey_ which uniquely identifies a _MessageProcessor_ that will handle processing that message.
2. The _MessageKey_ needs to have the appropriate identity semantics (_hashCode_ and _equals_)
3. In most cases when _Dempsy_ is distributed, the _Message_ needs to be serializable.

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
3. The `Word` class is serializable.

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
                // a node called word-count
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

