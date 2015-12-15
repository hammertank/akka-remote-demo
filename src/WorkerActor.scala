
import akka.actor.Actor
import com.typesafe.config.ConfigFactory

class WorkerActor extends Actor {
  
  val conf = ConfigFactory.load("worker")
  println(conf.getString("akka.remote.netty.tcp.hostname"))
  
  def receive = {
    case msg: String =>
      println(s"From LookupActor: $msg")
      sender() ! s"From WorkerActor: $msg"
  }
}