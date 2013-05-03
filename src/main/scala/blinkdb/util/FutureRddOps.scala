package blinkdb.util
import spark.RDD
import akka.dispatch.Future
import akka.dispatch.Dispatcher
import akka.dispatch.ExecutionContext

object FutureRddOps {
  implicit def toFutureRddOps[I](rdd: RDD[I]) = new FutureRddOps(rdd)
}

class FutureRddOps[I](rdd: RDD[I]) {
  def collectFuture()(implicit ec: ExecutionContext): Future[Array[I]] = {
    Future({ rdd.collect() })
  }
  
  def reduceFuture(reducer: (I, I) => I)(implicit ec: ExecutionContext): Future[I] = {
    Future({ rdd.reduce(reducer) })
  }
  
  def countFuture()(implicit ec: ExecutionContext): Future[Long] = {
    Future({ rdd.count() })
  }
}