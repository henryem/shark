package blinkdb.util
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

object CollectionUtils {
  implicit def toNestedOps[A](nestedSeq: Seq[Seq[A]]) = new NestedOps(nestedSeq)
  
  implicit def toDoublyNestedOps[A](doublyNestedSeq: Seq[Seq[Seq[A]]]) = new DoublyNestedOps(doublyNestedSeq)
  
  /** Some operations for conveniently handling nested sequences. */
  class NestedOps[A](nestedSeq: Seq[Seq[A]]) {
    def zipNested(nestedSeqB: Seq[Seq[A]]): Seq[Seq[(A,A)]] = {
      nestedSeq.zip(nestedSeqB).map(innerSeqs => innerSeqs._1.zip(innerSeqs._2))
    }
  
    def mapNested[B](mapper: A => B): Seq[Seq[B]] = {
      nestedSeq.map(_.map(mapper))
    }
  }
  
  /** Some operations for conveniently handling doubly-nested sequences. */
  class DoublyNestedOps[A](doublyNestedSeq: Seq[Seq[Seq[A]]]) {
    def reduceNested(aggregator: (A, A) => A): Seq[Seq[A]] = {
      doublyNestedSeq.reduce({(seqA, seqB) => seqA.zipNested(seqB).mapNested(tuple => aggregator(tuple._1, tuple._2))})
    }
    
    def aggregateNested[B](aggregator: Seq[A] => B): Seq[Seq[B]] = {
      //TODO: This is horribly inefficient.
      doublyNestedSeq
        .map(_.mapNested(item => List(item)))
        .reduceNested(_ ++ _)
        .mapNested(aggregator)
    }
  }
  
  /** 
   * Convert an Option for a Future into a Future for an Option.
   * 
   * Akka's Future.sequence() is supposed to handle this kind of thing, but it
   * does not work for Option; probably there is an elegant way to make it
   * work, in which case this method should go away.
   */
  def sequence[A](futureOption: Option[Future[A]])(implicit ec: ExecutionContext): Future[Option[A]] = {
    if (futureOption.isDefined) {
      futureOption.get.map(item => Some(item))
    } else {
      Future{None}
    }
  }
}