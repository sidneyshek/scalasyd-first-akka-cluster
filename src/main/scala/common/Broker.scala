package common

import scala.collection.immutable.Queue
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import akka.contrib.pattern.DistributedPubSubMediator.Put
import scala.concurrent.duration.{FiniteDuration, Deadline}
import akka.actor.Props
import com.github.nscala_time.time.Imports._
import java.util.concurrent.TimeUnit
import akka.pattern._

object Broker {

  def props(config: BrokerConfig): Props =
    Props(classOf[Broker], config)

  case class Ack(workId: String)

  private sealed trait WorkerStatus
  private case object Idle extends WorkerStatus
  private case class Busy(work: Work, deadline: Deadline) extends WorkerStatus
  private case class WorkerState(ref: ActorRef, status: WorkerStatus)
 
  private case object CleanupTick
  private case object CleanupIdsTick
}

class Broker(config: BrokerConfig) extends Actor with ActorLogging {
  import Broker._
  import BrokerWorkerProtocol._
  val ResultsTopic = config.resultTopic

  val maxRetries = config.maxRetries

  val workTimeout = FiniteDuration(config.workTimeoutSecs, TimeUnit.SECONDS)

  val mediator = DistributedPubSubExtension(context.system).mediator

  mediator ! Put(self)

  // FIX - make these queues persistent
  private var workers = Map[String, WorkerState]()
  private var pendingWork = Queue[Work]()
  private var workIds = Map[String, DateTime]()

  import context.dispatcher
  val cleanupTask = context.system.scheduler.schedule(workTimeout / 2, workTimeout / 2,
    self, CleanupTick)

  val cleanupIdsTask = context.system.scheduler.schedule(FiniteDuration(24, TimeUnit.HOURS),
    FiniteDuration(24, TimeUnit.HOURS), self, CleanupIdsTick)

  override def postStop(): Unit = {
    cleanupTask.cancel()
    cleanupIdsTask.cancel()
  }

  println("***** STARTED BROKER *****")

  def receive = {
    /********************************************
     * 0. Worker registration / de-registration *
     ********************************************/
    case RegisterWorker(workerId) =>
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(ref = sender))
      } else {
        println(s"Worker registered ${workerId}")
        log.debug("Worker registered: {}", workerId)
        workers += (workerId -> WorkerState(sender, status = Idle))
        if (pendingWork.nonEmpty)
          sender ! WorkIsReady
      }

    case DeregisterWorker(workerId) =>
      workers -= workerId

    /********************************************
     * 2. Worker requests work                  *
     ********************************************/
    case WorkerRequestsWork(workerId) =>
      if (pendingWork.nonEmpty) {
        workers.get(workerId) match {
          case Some(s @ WorkerState(_, Idle)) =>
            val (work, rest) = pendingWork.dequeue
            pendingWork = rest
            log.debug("Giving example {} some work {}", workerId, work.job)
            // TODO store in Eventsourced
            sender ! work
            workers += (workerId -> s.copy(status = Busy(work, Deadline.now + workTimeout)))
          case _ =>
            log.warning("Unregistered example {} requested work. Registering...", workerId)
            workers += (workerId -> WorkerState(sender, status = Idle))
        }
      }

    /********************************************
     * 5. Worker responds with Success/Failed   *
     ********************************************/
    case WorkIsDone(workerId, workId, result) =>
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work, _))) if work.workId == workId =>
          log.debug("Work is done: {} => {} by example {}", work, result, workerId)
          // TODO store in Eventsourced
          workers += (workerId -> s.copy(status = Idle))
          mediator ! DistributedPubSubMediator.Publish(ResultsTopic, WorkResult(workId, result))
          sender ! BrokerWorkerProtocol.Ack(workId)
        case _ =>
          if (workIds.contains(workId)) {
            // previous Ack was lost, confirm again that this is done
            sender ! BrokerWorkerProtocol.Ack(workId)
          }
      }

    case WorkFailed(workerId, workId) =>
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work, _))) if work.workId == workId =>
          log.info("Work failed: {}", work)
          // TODO store in Eventsourced
          workers += (workerId -> s.copy(status = Idle))
          pendingWork = pendingWork enqueue work
          notifyWorkers()
        case _ =>
      }

    /********************************************
     * Client sends work to broker              *
     ********************************************/
    case work: Work =>
      // idempotent
      if (workIds.contains(work.workId)) {
        sender ! Broker.Ack(work.workId)
      } else {
        println(s"Accepted work ${work.workId}")
        log.debug("Accepted work: {}", work)
        // TODO store in Eventsourced
        pendingWork = pendingWork enqueue work
        workIds += (work.workId -> DateTime.now)
        sender ! Broker.Ack(work.workId)
        notifyWorkers()
      }

    /********************************************
     * General housekeeping stuff               *
     ********************************************/
    case CleanupTick =>
      for ((workerId, s @ WorkerState(_, Busy(work, timeout))) <- workers) {
        if (timeout.isOverdue) {
          log.info("Work timed out: {}", work)
          // TODO store in Eventsourced
          workers -= workerId
          pendingWork = pendingWork enqueue work
          notifyWorkers()
        }
      }

    case CleanupIdsTick =>
      log.info("Cleaning up old work Ids")
      workIds = workIds.filter(kv => {
        import com.github.nscala_time.time.Imports._
        val (_, lastLodged) = kv
        DateTime.now < lastLodged + 24.hours
      })

  }

  /********************************************
   * 1. Notify workers that work is ready     *
   ********************************************/
  def notifyWorkers(): Unit =
    if (pendingWork.nonEmpty) {
      // could pick a few random instead of all
      workers.foreach {
        case (id, WorkerState(ref, Idle)) =>
          implicit val timeout = akka.util.Timeout(5, TimeUnit.SECONDS)
          (ref ? WorkIsReady) recover { case t => {
              log.warning("Registered example unavailable {}", id)
              self ! DeregisterWorker(id)
            }
          }
        case _                           => // busy
      }
    }
}