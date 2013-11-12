package example

/**
 * Types of work jobs that the workers currently support.
 */
object WorkTypes {
  case class SendNotifications(value: Int)
  case class AggregateData(value: Int)
}
