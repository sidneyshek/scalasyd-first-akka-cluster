package example

import common._
import scala.concurrent.duration._
import akka.actor._
import akka.testkit.TestProbe
import akka.cluster.Cluster
import org.specs2.mutable._
import org.specs2.time.NoTimeConversions
import akka.contrib.pattern.{ClusterClient, DistributedPubSubExtension, ClusterSingletonManager}
import akka.contrib.pattern.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import akka.pattern._
import akka.util.Timeout

object DistributedWorkerSpec {

  /**
   * Instantiate a FlakyWorkExecutor to test
   */
  class FlakyWorkExecutor extends Actor {
    var i = 0

    override def postRestart(reason: Throwable): Unit = {
      i = 3
      super.postRestart(reason)
    }

    def receive = {
      case n: Int ⇒
        i += 1
        if (i == 3) throw new RuntimeException("Flaky worker")
        if (i == 5) context.stop(self)

        val n2 = n * n
        val result = s"$n * $n = $n2"
        sender ! Worker.WorkComplete(result)
    }
  }
  class DummyWorkExecutor extends Actor {
    def receive = {
      case n: Int ⇒
        val n2 = n * n
        val result = s"$n * $n = $n2"
        sender ! Worker.WorkComplete(result)
    }
  }
}

class DistributedWorkerSpec extends Specification with NoTimeConversions {
  sequential
  import DistributedWorkerSpec._

  val workTimeout = 3.seconds

  "Distributed workers" should {
    "perform work and publish results" in new test.AkkaTestkitSpecs2Support {
      val clusterAddress = Cluster(system).selfAddress
      Cluster(system).join(clusterAddress)
      val masterConfig = BrokerConfig.default.copy(retrySecs = 0)
      system.actorOf(ClusterSingletonManager.props(_ ⇒ Broker.props(masterConfig), "active",
        PoisonPill, None), "broker")

      val initialContacts = Set(
        system.actorSelection(RootActorPath(clusterAddress) / "user" / "receptionist"))
      val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clusterClient")

      val frontend = system.actorOf(Frontend.props(clusterClient), "frontend")

      val results = TestProbe()
      DistributedPubSubExtension(system).mediator ! Subscribe(masterConfig.resultTopic, results.ref)
      expectMsgType[SubscribeAck]

      for (n ← 1 to 3)
        system.actorOf(Worker.props(clusterClient, Props[DummyWorkExecutor], 1.second), "worker-" + n)

      val flakyWorker = system.actorOf(Worker.props(clusterClient, Props[FlakyWorkExecutor], 1.second), "flaky-worker")

      // might take a while for things to get connected
      within(10.seconds) {
        awaitAssert {
          frontend ! Work("1", 1)
          expectMsg(Frontend.Ok)
        }
      }

      results.expectMsgType[WorkResult].workId must_== "1"

      for (n ← 2 to 100) {
        frontend ! Work(n.toString, n)
        expectMsg(Frontend.Ok)
      }

      results.within(20.seconds) {
        val ids = results.receiveN(99).map { case WorkResult(workId, _) ⇒ workId }
        // nothing lost, and no duplicates
        ids.toVector.map(_.toInt).sorted must containAllOf((2 to 100).toSeq).inOrder
      }

    }
    "perform work via WorkProducer and publish results" in new test.AkkaTestkitSpecs2Support {
      val clusterAddress = Cluster(system).selfAddress
      Cluster(system).join(clusterAddress)
      val masterConfig = BrokerConfig.default.copy(retrySecs = 0)
      system.actorOf(ClusterSingletonManager.props(_ ⇒ Broker.props(masterConfig), "active",
        PoisonPill, None), "broker")

      val initialContacts = Set(
        system.actorSelection(RootActorPath(clusterAddress) / "user" / "receptionist"))
      val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "clusterClient")

      val frontend = system.actorOf(Frontend.props(clusterClient), "frontend")

      val results = TestProbe()
      DistributedPubSubExtension(system).mediator ! Subscribe(masterConfig.resultTopic, results.ref)
      expectMsgType[SubscribeAck]

      for (n ← 1 to 3)
        system.actorOf(Worker.props(clusterClient, Props[DummyWorkExecutor], 1.second), "worker-" + n)

      val flakyWorker = system.actorOf(Worker.props(clusterClient, Props[FlakyWorkExecutor], 1.second), "flaky-worker")

      val workProducer = system.actorOf(WorkProducer.props(frontend), "producer")

      // might take a while for things to get connected
      within(10.seconds) {
        awaitAssert {
          frontend ! Work("1", 1)
          expectMsg(Frontend.Ok)
        }
      }

      results.expectMsgType[WorkResult].workId must_== "1"
      for (n ← 2 to 100) {
        workProducer ! n
      }

      results.within(20.seconds) {
        val ids = results.receiveN(99).map { case WorkResult(_, str) ⇒ {
          str
        } }
        // nothing lost, and no duplicates
        ids.toVector must containAllOf((2 to 100).toSeq.map(n => {
          val n2 = n * n
          s"$n * $n = $n2"
        }))
      }
    }
  }

}