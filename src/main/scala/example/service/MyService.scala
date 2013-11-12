package example
package service

import scalaz._, Scalaz._
import effect._

object MyService {
  /**
   * Example service functions.
   */
  def notificationEmailService(value: Int): Service[Int] = {
    println(s"Sending notification for data: ${value}")
    (value + 1).success[String].pure[IO]
  }

  def aggregateDataService(value: Int): Service[Int] = {
    println(s"Aggregating data: ${value}")
    (value * 2).success[String].pure[IO]
  }

}
