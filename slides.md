## My First Akka Cluster

##### ScalaSyd Nov 2013

##### Sidney Shek (@sidneyshek)



## The Problem

Resilient and scalable asynchronous task processing for...

   * Sending emails

   * Processing incoming emails

   * Background data aggregation



## Akka in a minute 

> Akka is a toolkit and runtime for building highly concurrent, distributed, and fault tolerant event-driven applications on the JVM.


### Meaning...

   * Libraries and patterns for building messaging/distributed processing systems

   * Actors as the code interface


## Actors in a minute

Actors are **objects** that do something in response to messages

    case class Greeting(who: String)
 
    class GreetingActor extends Actor with ActorLogging {
      def receive = {
        case Greeting(who) => log.info("Hello " + who)
      }
    }
 
    val system = ActorSystem("MySystem")
    val greeter = system.actorOf(Props[GreetingActor], name = "greeter")
    greeter ! Greeting("Charlie Parker")


### Meaning...

   * Actors have state (explicit or state machine)

   * No (easy) type safety in messages



## What we're building

   * Like the [Paranoid Pirate](http://zguide.zeromq.org/php:chapter4#Robust-Reliable-Queuing-Paranoid-Pirate-Pattern) pattern


### Broker-Worker protocol


#### Registration

1. Worker sends RegisterWorker to broker


#### Work to do

1. Broker sends WorkIsReady to registered workers

2. Free workers send WorkerRequestsWork to broker

3. Broker sends Work to worker

4. Worker sends WorkIsDone or WorkFailed


### Client-Broker protocol

1. Client sends Work to broker

2. Broker saves work to queue and Acks



## Let's code!


### Broker key points

   * Cluster Singleton pattern

      * Single instance

      * NOT single threaded


### Worker key points

   * Supervisor

   * State machine for replying to broker

   * Automatic reconnect with Cluster Client


### Client key points

   * Actor/FP boundary


### Extension - Broker retries


### Extension - Removing dead workers


### What's next

   * Orderly shutdown with Reaper pattern

   * Queue persistence

   * 'Service orientation'

   * Custom serialisation for message versioning 




## Was it worth it?


### Akka provides:

   * Supervision, state machines and thread management

   * Lots of patterns

   * Focus on message processing over plumbing


### However...

   * No type safety for messages

   * Not really functional

   * No silver bullet for distributed systems and multi-threading:

        * Extra config and complexity

        * Debugging



## What else could we have done?

   * Combine Akka with something like RabbitMQ

   * Write from scratch with something like ZeroMQ 

   * Write using something like Twitter Finagle


## Akka vs RabbitMQ vs ZeroMQ



## Review and recap

We created a (fairly) reliable cluster for async task processing!

**It was worth it (for now)**

