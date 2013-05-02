package blinkdb.util

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
  
}