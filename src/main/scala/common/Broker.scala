package common

import scala.collection.immutable.Queue
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import akka.contrib.pattern.DistributedPubSubMediator.Put
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import akka.actor.Props
import com.github.nscala_time.time.Imports._
import java.util.concurrent.TimeUnit

/**
 * The Broker represents the 'backend' that coordinates all async jobs. It queues up all pending work, and is also
 * responsible for retrying work that has failed. It is also responsible for holding a list of registered workers.
 *
 * It receives work of type Work, and responds with Broker.Ack when it has successfully queued the work.
 *
 * There should only be one broker active at any one time (ensured by starting up a broker using ClusterSingletonManager)
 */
object Broker {

  def props(config: BrokerConfig): Props =
    Props(classOf[Broker], config)

  case class Ack(workId: String)

  private sealed trait WorkerStatus
  private case object Idle extends WorkerStatus
  private case class Busy(work: RetryWork, deadline: Deadline) extends WorkerStatus
  private case class WorkerState(ref: ActorRef, status: WorkerStatus)

  private case object CleanupTick
  private case class RetryWork(work: Work, count: Int = 0)

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

  private var workers = Map[String, WorkerState]()

  // FIX - make these queues persistent
  private var pendingWork = Queue[RetryWork]()
  private var retryingWork = Queue[RetryWork]()
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

  // FIX - on startup, reload the pendingWork and retriedWork queues
  println("***** STARTED BROKER *****")
  def receive = {
    /**********************************************
     * 0. Registration/De-registration of workers *
     **********************************************/
    case RegisterWorker(workerId) =>
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(ref = sender))
      } else {
        log.debug("Worker registered: {}", workerId)
        workers += (workerId -> WorkerState(sender, status = Idle))
        if (pendingWork.nonEmpty)
          sender ! WorkIsReady
      }

    case DeregisterWorker(workerId) =>
      workers -= workerId

    /**********************************************
     * 2. Worker requests work                    *
     **********************************************/
    case WorkerRequestsWork(workerId) =>
      if (pendingWork.nonEmpty) {
        workers.get(workerId) match {
          case Some(s @ WorkerState(_, Idle)) =>
            val (work, rest) = pendingWork.dequeue
            pendingWork = rest
            log.debug("Giving worker {} some work {}", workerId, work.work.job)
            sender ! work.work
            workers += (workerId -> s.copy(status = Busy(work, Deadline.now + workTimeout)))
          case _ =>
            log.warning("Unregistered worker {} requested work", workerId)
            self ! RegisterWorker(workerId)
        }
      }

    /**********************************************
     * 5. Worker responds with Complete/Failed    *
     **********************************************/
    case WorkIsDone(workerId, workId, result) =>
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work, _))) if work.work.workId == workId =>
          log.debug("Work is done: {} => {} by worker {}", work, result, workerId)
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
        case Some(s @ WorkerState(_, Busy(work, _))) if work.work.workId == workId =>
          log.info("Work failed: {}", work)
          workers += (workerId -> s.copy(status = Idle))
          if (work.count < maxRetries) {
            val newWork = work.copy(count = work.count + 1)
            retryingWork = retryingWork enqueue newWork
            context.system.scheduler.scheduleOnce(FiniteDuration(config.retrySecs, TimeUnit.SECONDS),
              self, newWork)

          } else {
            log.info("Retry count for work exceeded maximum: {}", work)
          }

          sender ! BrokerWorkerProtocol.Ack(workId)
        case _ =>
          log.warning("Expected response from worker {}", workerId)
      }

    /**********************************************
     * Client sends work to broker                *
     **********************************************/
    case work: Work =>
      // idempotent
      if (workIds.contains(work.workId)) {
        sender ! Broker.Ack(work.workId)
      } else {
        log.debug("Accepted work: {}", work)
        pendingWork = pendingWork enqueue RetryWork(work)
        workIds += (work.workId -> DateTime.now)
        sender ! Broker.Ack(work.workId)
        notifyWorkers()
      }

    /***********************************************
     * General housekeeping to remove dead workers *
     ***********************************************/
    case CleanupTick =>
      for ((workerId, s @ WorkerState(_, Busy(work, timeout))) ← workers) {
        if (timeout.isOverdue) {
          log.info("Work timed out: {}", work)
          workers -= workerId
          pendingWork = pendingWork enqueue work
          notifyWorkers()
        }
      }

    /***********************************************
     * Reschedule work that failed                 *
     ***********************************************/
    case r @ RetryWork(work, count) =>
      log.info("Rescheduling work {}", r)
      pendingWork = pendingWork enqueue r
      workIds += (work.workId -> DateTime.now)
      retryingWork = retryingWork.filterNot(_.work.workId == r.work.workId)
      notifyWorkers()

    /***********************************************
     * General housekeeping to remove old work Ids *
     ***********************************************/
    case CleanupIdsTick =>
      log.info("Cleaning up old work Ids")
      workIds = workIds.filter(kv => {
        import com.github.nscala_time.time.Imports._
        val (_, lastLodged) = kv
        DateTime.now < lastLodged + 24.hours
      })
  }

  def notifyWorkers(): Unit =
    if (pendingWork.nonEmpty) {
      // could pick a few random instead of all
      workers.foreach {
        case (_, WorkerState(ref, Idle)) => ref ! WorkIsReady
        case _                           => // busy
      }
    }
}