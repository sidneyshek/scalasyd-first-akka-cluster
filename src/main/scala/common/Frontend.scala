package common

import scala.concurrent.duration._
import akka.actor.{Props, ActorRef, Actor}
import akka.pattern._
import akka.util.Timeout
import akka.contrib.pattern.ClusterClient.SendToAll

object Frontend {
  def props(clusterClient: ActorRef): Props = Props(classOf[Frontend], clusterClient)
  case object Ok
  case class NotOk(t: Throwable, work: Any)
}

/**
 * Frontend is responsible for attempting to send work requests to the broker. It responds with
 * either Ok or NotOk
 */
class Frontend(clusterClient: ActorRef) extends Actor {
  import Frontend._
  import context.dispatcher

  def receive = {
    case work =>
      implicit val timeout = Timeout(5.seconds)
      (clusterClient ? SendToAll("/user/broker/active", work)) map {
        case Broker.Ack(i) => Ok
      } recover { case t => NotOk(t, work) } pipeTo sender
  }

}