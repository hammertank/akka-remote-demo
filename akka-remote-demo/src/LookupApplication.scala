

import scala.concurrent.duration.DurationInt
import scala.util.Random

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala

object LookupApplication {
  def main(args: Array[String]) {
    if (args.isEmpty || args.head == "Worker")
      startRemoteWorkerSystem()
    if (args.isEmpty || args.head == "Lookup")
      startRemoteLookupSystem()
  }

  def startRemoteWorkerSystem(): Unit = {
    val system = ActorSystem("WorkerSystem",
      ConfigFactory.load("worker"))
    val actorRef = system.actorOf(Props[WorkerActor], "worker")
    
    println(actorRef)
    
    println("Started WorkerSystem - waiting for messages")
  }

  def startRemoteLookupSystem(): Unit = {
    val system = ActorSystem("LookupSystem", ConfigFactory.load("lookup"))
    val remotePath = "akka.tcp://WorkerSystem@node3:2552/user/worker"
    val actor = system.actorOf(Props(classOf[LookupActor], remotePath), "LookupActor")

    println("Started LookupSystem")

    import system.dispatcher
    system.scheduler.schedule(1.seconds, 1.seconds) {
      actor ! Random.nextInt(100).toString
    }
  }
}