package common

import java.util.UUID
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.duration._
import akka.actor._

object WorkProducer {
  def props(frontend: ActorRef): Props = Props(classOf[WorkProducer], frontend)
}

/**
 * The WorkProducer wraps work requests with a unique Id and is responsible for retrying
 * sending of requests to the broker
 */
class WorkProducer(frontend: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher
  def scheduler = context.system.scheduler
  def rnd = ThreadLocalRandom.current
  def nextWorkId(): String = UUID.randomUUID().toString

  def receive = {
    case w =>
      val work = Work(nextWorkId(), w)
      log.debug("Produced work: {}", work)
      frontend ! work
      context.become(waitAccepted(work), discardOld = false)
  }

  def waitAccepted(work: Work): Actor.Receive = {
    case Frontend.Ok =>
      context.unbecome()
    case Frontend.NotOk(_, _) =>
      log.info("Work not accepted, retry after a while")
      scheduler.scheduleOnce(3.seconds, frontend, work)
    case e => log.warning("Unexpected message {}", e)
  }

}