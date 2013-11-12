package common

case class BrokerConfig(hostname: String, port: Int, role: String = "broker", workTimeoutSecs: Int = 10,
                        retrySecs: Int = 10, maxRetries: Int = 10, resultTopic: String = "results")

object BrokerConfig {
  val default = BrokerConfig("127.0.0.1", 2552)
}