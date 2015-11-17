package my.spark.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import my.spark.socket.SimpleSocketServer
import my.spark.util.ConfigUtils
import my.spark.util.DFSUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.Logging
import my.spark.socket.SimpleSocketServer
import java.net.BindException
import my.spark.socket.SimpleSocketServer
import my.spark.socket.SimpleSocketServer
import java.net.Socket
import java.io.DataInputStream
import java.io.DataOutputStream

class ShutdownServer(startPort: Int, maxRetries: Int, ssc: StreamingContext) extends Logging {

  private var port = startPort

  val shutdownFunc = (socket: Socket) => {
    val dataInStream = new DataInputStream(socket.getInputStream)
    val dataOutStream = new DataOutputStream(socket.getOutputStream)
    val cmd = dataInStream.readUTF()

    val (ret, response) = try {
      if (cmd == "stop") {
        logInfo("Received signal 'stop'. Start to stop streaming context gracefully.")
        ssc.stop(true, true)
        (true, "Shutdown complete.")
      } else if (cmd == "kill") {
        logInfo("Received signal 'kill'. Start to stop sparkContext without stopping streaming context. Unprocessed data will be lost.")
        ssc.sparkContext.stop
        (true, "Shutdown complete.")
      } else {
        (false, "Invalid command: $cmd.")
      }
    } catch {
      case e: Exception =>
        logError("Shutdown with exception", e)
        (true, "Shutdown complete with errors.")
    }

    dataOutStream.writeUTF(response)
    dataOutStream.close

    if (cmd == "kill") {
      sys.exit // Force master container to exit
    }

    ret
  }

  val serverSocket = createServer

  private def createServer: SimpleSocketServer = {
    for (offset <- 0 to maxRetries) {
      try {
        // Do not try a privilege port
        port = ((startPort + offset - 1024) % (65536 - 1024)) + 1024
        val server = new SimpleSocketServer(shutdownFunc, port)
        logInfo(s"Successfully started ShutdownServer on port $port.")
        return server
      } catch {
        case ex: BindException if isBindCollision(ex) =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"${ex.getMessage}: ShutdownServer failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            exception.setStackTrace(ex.getStackTrace)
            throw exception
          }

          logWarning(s"ShutdownServer could not bind on port $port. " +
            s"Attempting port ${port + 1}.")
      }
    }

    // Should never happen
    throw new Exception(s"Failed to create ShutdownServer on port $port")
  }

  private def saveShutdownServerInfo {
    val host = ssc.sparkContext.getConf.get("spark.driver.host")
    DFSUtils.save(ShutdownServer.serverInfoFile, host + ":" + port)
  }

  def start {
    serverSocket.start

    saveShutdownServerInfo
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   *
   * From org.apache.spark.util.Utils
   */
  private def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _            => false
    }
  }
}

object ShutdownServer {
  val checkpoitDir = ConfigUtils.getString("application.checkpoint", null)

  if (checkpoitDir == null) throw new Exception("Property 'application.checkpoint' can not be null.")

  private val serverInfoFile = {
    new Path(checkpoitDir, "server_info").toString
  }

  def shudownServerInfo(fs: FileSystem = FileSystem.get(new Configuration)) = {
    val serverInfoStr = DFSUtils.read(ShutdownServer.serverInfoFile, 1, fs)(0)
    val Array(hostStr, portStr) = serverInfoStr.split(":")
    (hostStr, portStr.toInt)
  }

}

object ShutdownServerTest {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(new SparkConf, Seconds(10))

    val host = ssc.sparkContext.getConf.get("spark.driver.host")
    val lines = ssc.socketTextStream(host, 9999)
    lines.foreachRDD(rdd => rdd.foreach(println))

    val shutdownServer = new ShutdownServer(7788, 10, ssc)
    shutdownServer.start

    ssc.start
    ssc.awaitTermination
  }
}