package example

import akka.actor.ActorSystem
import akka.contrib.pattern.ClusterClient
import scalaz._, syntax.std.boolean._, syntax.id._
import common.Worker

class WorkerMain extends Startup {

  def main(args: Array[String]): Unit = {
    startWorker(1)
  }

  def startWorker(numWorkers: Int): Unit = {
    val system = ActorSystem(systemName)
    val initialContacts = seedNodes.map(addr => {
      val trimmedAddr = addr.endsWith("/").fold(addr.substring(0, addr.length - 1), addr)
      trimmedAddr + "/user/receptionist"
    }).map(system.actorSelection).toSet

    /**
     * Use a ClusterClient to connect with the broker.
     * ClusterClient is responsible for connecting/reconnecting to the cluster
     */
    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clusterClient")
    for (n <- 1 to numWorkers)
      system.actorOf(Worker.props(clusterClient, WorkExecutor.props), "example-" + n)

    println("Started example worker")
  }

}
