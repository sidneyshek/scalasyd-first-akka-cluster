package example

trait Startup {
  // FIX - put these into a config file
  val systemName = "MyWorkers"
  val seedNodes = List("akka.tcp://MyWorkers@127.0.0.1:2551", "akka.tcp://MyWorkers@127.0.0.1:2552")
}
