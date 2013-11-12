package common

import java.util.UUID
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.duration._
import akka.actor._

object WorkProducer {
  def props(frontend: ActorRef, retrySecs: Int): Props = Props(classOf[WorkProducer], frontend, retrySecs)
}

/**
 * Work producer is responsible for retrying sending of messages to the master. The Frontend does the actual sending
 * and returns Ok/NotOk. If we get a NotOk, then the WorkProducer schedules a retry.
 *
 * @param frontend reference to frontend actor that does the actual sending
 * @param retrySecs seconds between retries of sending messages
 */
class WorkProducer(frontend: ActorRef, retrySecs: Int) extends Actor with ActorLogging {

  import context.dispatcher

  def scheduler = context.system.scheduler

  def rnd = ThreadLocalRandom.current

  def nextWorkId(): String = UUID.randomUUID().toString

  val retry = retrySecs.seconds

  def receive = {
    case PoisonPill =>
      context.stop(self)
    case Frontend.Ok =>
    case Frontend.NotOk(t, work) =>
      log.info("Work not accepted, retry after a while. Error: {}", t.getMessage)
      scheduler.scheduleOnce(retry, frontend, work)
    case event ⇒
      val work = Work(nextWorkId(), event)
      log.debug("Produced work: {}", work)
      frontend ! work
  }
}