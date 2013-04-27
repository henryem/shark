package blinkdb.util
import spark.RDD
import akka.dispatch.Future
import akka.dispatch.Dispatcher
import akka.dispatch.ExecutionContext
import spark.SparkEnv

object FutureRddOps {
  implicit def toFutureRddOps[I](rdd: RDD[I]) = new FutureRddOps(rdd)
}

class FutureRddOps[I](rdd: RDD[I]) {
  def collectFuture()(implicit ec: ExecutionContext): Future[Array[I]] = {
    //A workaround for SPARK-534:
    // https://spark-project.atlassian.net/browse/SPARK-534
    val env = SparkEnv.get
    Future({
      SparkEnv.set(env)
      rdd.collect()
    })
  }
  
  def reduceFuture(reducer: (I, I) => I)(implicit ec: ExecutionContext): Future[I] = {
    val env = SparkEnv.get
    Future({
      SparkEnv.set(env)
      rdd.reduce(reducer)
    })
  }
  
  def countFuture()(implicit ec: ExecutionContext): Future[Long] = {
    val env = SparkEnv.get
    Future({
      SparkEnv.set(env)
      rdd.count()
    })
  }
}