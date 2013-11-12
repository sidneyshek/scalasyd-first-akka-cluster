package example

import service._
import akka.actor.{Props, Actor}
import akka.pattern._
import common.Worker
import scala.concurrent._
import scalaz._, Scalaz._

object WorkExecutor {
  def props(): Props = Props(classOf[WorkExecutor])
}

/**
 * The WorkExecutor acts as the interface between the Actor system and the rest of the app code.
 *
 * Think of it like a dispatch router in a web app.
 */
class WorkExecutor extends Actor {
  import ExecutionContext.Implicits.global

  def receive = {
    case WorkTypes.SendNotifications(e) => {
      runService[Int](sendError)(successHandler)(MyService.notificationEmailService(e)) pipeTo sender
    }
    case WorkTypes.AggregateData(e) => {
      runService[Int](sendError)(successHandler)(MyService.aggregateDataService(e)) pipeTo sender
    }
    case _ =>
      sender ! Worker.WorkError("Unknown work type")
  }

  def sendError(msg: Option[String]): Future[Worker.WorkResponse] =
    future { Worker.WorkError(msg, None) }

  def successHandler[A](value: A): Future[Worker.WorkResponse] =
    future { Worker.WorkComplete(value) }

  def sendUnexpectedError: Future[Worker.WorkResponse] =
    future { Worker.WorkError("Unexpected response running the service.") }

  def runService[A](errorHandler: Option[String] => Future[Worker.WorkResponse])
                   (successHandler: A => Future[Worker.WorkResponse])
                   (service: Service[A]): Future[Worker.WorkResponse] = {
    val result = service.unsafePerformIO()
    result match {
      case Success(v) => successHandler(v)
      case Failure(e) => errorHandler(Some(e))
    }
  }


}