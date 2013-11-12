package example

import com.typesafe.config.ConfigFactory
import akka.actor.{Props, ActorSystem}
import common.{WorkProducer, Frontend}
import scalaz._, Scalaz._
import akka.contrib.pattern.ClusterClient

class ClientApi extends Startup {
  private val (system, workProducer, frontend) = startSystem

  def startSystem = {
    val seedNodesString = seedNodes.map("\"" + _ + "\"").mkString("[", ",", "]")
    val conf = ConfigFactory.parseString(
      s"akka.cluster.seed-nodes=$seedNodesString").withFallback(ConfigFactory.load())

    val system = ActorSystem(systemName)
    val initialContacts = seedNodes.map(addr => {
      val trimmedAddr = addr.endsWith("/").fold(addr.substring(0, addr.length - 1), addr)
      trimmedAddr + "/user/receptionist"
    }).map(system.actorSelection).toSet

    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clientApiClusterClient")
    val frontend = system.actorOf(Frontend.props(clusterClient), "frontend")
    val workProducer = system.actorOf(WorkProducer.props(frontend), "producer")
    (system, workProducer, frontend)
  }

  def sendNotifications(value: Int): Service[Unit] =
    wrapInService { workProducer ! WorkTypes.SendNotifications(value) }

  def aggregateData(value: Int): Service[Unit] =
    wrapInService { workProducer ! WorkTypes.AggregateData(value) }
}
