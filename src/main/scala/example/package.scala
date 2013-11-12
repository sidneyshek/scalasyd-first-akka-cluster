import scalaz._, Scalaz._
import effect._

package object example {
  /**
   * This is a simple wrapper around our app's 'service' functions
   */
  type Service[A] = IO[Validation[String, A]]

  def wrapInService[A](v: => A) = v.success[String].pure[IO]
}
