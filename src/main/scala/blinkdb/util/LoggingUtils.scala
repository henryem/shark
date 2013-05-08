package blinkdb.util
import shark.LogHelper
import java.util.concurrent.TimeUnit._

object LoggingUtils {
  def startCount(procedureName: String): PerformanceCounter = {
    new PerformanceCounter(procedureName)
  }
  
  class PerformanceCounter(private val procedureName: String) extends LogHelper {
    val startTimeNanos = System.nanoTime()
    
    def stop(): Unit = {
      val elapsedTimeNanos = System.nanoTime() - startTimeNanos
      //TODO: Use the log prefix of the class that called this.
      System.err.println("%s finished in %f milliseconds.".format(
          procedureName,
          elapsedTimeNanos / 1.0e6))
    }
  }
}