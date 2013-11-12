package common

object BrokerWorkerProtocol {
  // Messages from Workers
  case class RegisterWorker(workerId: String)
  case class WorkerRequestsWork(workerId: String)
  case class WorkIsDone(workerId: String, workId: String, result: Any)
  case class WorkFailed(workerId: String, workId: String)
  case class DeregisterWorker(workerId: String)

  // Messages to Workers
  case object WorkIsReady
  case class Ack(id: String)
}