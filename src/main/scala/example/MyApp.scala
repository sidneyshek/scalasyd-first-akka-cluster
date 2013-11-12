package example

class MyApp {
  def main(args: Array[String]): Unit = {
    var n = 0
    val sleepInterval = 5000

    val client = new ClientApi

    while (true) {
      println(s"Generating notification for data ${n}")
      client.sendNotifications(n)
      n += 1
      Thread.sleep(sleepInterval)
    }
  }
}
