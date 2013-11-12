package common

import scala.concurrent.duration._
import akka.actor.Actor
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Send
import akka.pattern._
import akka.util.Timeout

object Frontend {
  case object Ok
  case object NotOk
}

/**
 * Frontend is responsible for attempting to send work requests to the cluster master.
 */
class Frontend extends Actor {
  import Frontend._
  import context.dispatcher
  val mediator = DistributedPubSubExtension(context.system).mediator

  def receive = {
    case work =>
      implicit val timeout = Timeout(5.seconds)
      (mediator ? Send("/user/broker/active", work, localAffinity = false)) map {
        case Broker.Ack(_) => Ok
      } recover { case _ => NotOk } pipeTo sender

  }

}