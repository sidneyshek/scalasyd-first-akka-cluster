package example

import com.typesafe.config.ConfigFactory
import akka.actor.{Props, ActorSystem}
import common.{WorkProducer, Frontend}
import scalaz._, Scalaz._

class ClientApi extends Startup {
  private val (system, workProducer, frontend) = startSystem

  def startSystem = {
    val seedNodesString = seedNodes.map("\"" + _ + "\"").mkString("[", ",", "]")
    val conf = ConfigFactory.parseString(
      s"akka.cluster.seed-nodes=$seedNodesString").withFallback(ConfigFactory.load())

    val system = ActorSystem(systemName, conf)
    val frontend = system.actorOf(Props[Frontend], "frontend")
    val workProducer = system.actorOf(WorkProducer.props(frontend), "producer")
    (system, workProducer, frontend)
  }

  def sendNotifications(value: Int): Service[Unit] =
    wrapInService { workProducer ! WorkTypes.SendNotifications(value) }

  def aggregateData(value: Int): Service[Unit] =
    wrapInService { workProducer ! WorkTypes.AggregateData(value) }
}
