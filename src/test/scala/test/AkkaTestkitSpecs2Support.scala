package test

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import org.specs2.mutable.After


/**
 * A tiny class that can be used as a Specs2 'context'.
 * Taken from http://blog.xebia.com/2012/10/01/testing-akka-with-specs2/
 */
abstract class AkkaTestkitSpecs2Support extends TestKit(ActorSystem()) with After with ImplicitSender {
  // make sure we shut down the actor system after all tests have run
  def after = system.shutdown()
}
