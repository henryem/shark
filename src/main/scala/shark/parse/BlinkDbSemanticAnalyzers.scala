package shark.parse

import org.slf4j.Logger
import org.apache.hadoop.hive.conf.HiveConf
import spark.RDD
import shark.execution.HiveOperator
import org.apache.hadoop.hive.ql.parse.ASTNode
import shark.execution.CacheSinkOperator
import shark.execution.RddScanOperator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.exec.OperatorFactory
import shark.execution.IntermediateCacheOperator
import spark.storage.StorageLevel
import scala.collection.JavaConversions
import shark.execution.TerminalOperator

class InputExtractionSemanticAnalyzer(conf: HiveConf) extends SharkSemanticAnalyzer(conf) {
  //HACK: This should be part of a proper API.
  var intermediateInputOperator: shark.execution.Operator[_] = _
  
  override def executePostAnalysisHooks(terminalOps: Seq[TerminalOperator]): Seq[TerminalOperator] = {
    val topOperators = terminalOps.flatMap(_.returnTopOperators()).distinct
    val postInputScanOperators = BlinkDbSemanticAnalyzers.getPostInputScanOperators(topOperators)
    // Cut the graph here.  Insert an IntermediateCacheOperator that will
    // produce a cached version of the RDD at the cut.  Later we will find
    // this node and execute it, discarding the rest of the graph; for now
    // it is too difficult to actually remove the rest of the graph, so we
    // rely on downstream code to find and execute the caching node.
    require(postInputScanOperators.size == 1)
    val postInputScanOperatorAndChildren = postInputScanOperators.toSeq.apply(0)
    require(postInputScanOperatorAndChildren._2.size == 1)
    val parent = postInputScanOperatorAndChildren._1
    val child = postInputScanOperatorAndChildren._2.apply(0)
    val cacheSinkOp = BlinkDbSemanticAnalyzers.insertCacheOperator(child, parent)
    this.intermediateInputOperator = cacheSinkOp
    terminalOps
  }
}

class BootstrapSemanticAnalyzer(conf: HiveConf, inputRdd: RDD[Any]) extends SharkSemanticAnalyzer(conf) {
  override def executePostAnalysisHooks(terminalOps: Seq[TerminalOperator]): Seq[TerminalOperator] = {
    val topOperators = terminalOps.flatMap(_.returnTopOperators()).distinct
    val postInputScanOperators = BlinkDbSemanticAnalyzers.getPostInputScanOperators(topOperators)
    // Cut the graph here.  Insert an RddScanOperator parameterized
    // by inputRdd; this will short-circuit the top part of the graph and
    // start the computation at this node, using inputRdd.  Return the old
    // terminal operators.
    require(postInputScanOperators.size == 1)
    val postInputScanOperatorAndChildren = postInputScanOperators.toSeq.apply(0)
    require(postInputScanOperatorAndChildren._2.size == 1)
    val parent = postInputScanOperatorAndChildren._1
    val child = postInputScanOperatorAndChildren._2.apply(0)
    BlinkDbSemanticAnalyzers.insertRddScanOperator(child, parent, inputRdd)
    terminalOps
  }
}

object BlinkDbSemanticAnalyzers {
  /**
   * A map from each post-input scan operator in the operator graph to its
   * child or children in the operator graph.  For example, a typical graph
   * will look like this:
   * 
   * ( TableScanOperator )
   *           |
   * ( SelectOperator    )
   *           |
   * ( GroupByOperator   )
   *           |
   * ( CacheSinkOperator )
   * 
   * In this example, this method will return a map from the SelectOperator to
   * a singleton list containing the GroupByOperator.
   */
  def getPostInputScanOperators(topOperators: Seq[shark.execution.Operator[_ <: HiveOperator]]): Map[shark.execution.Operator[_], Seq[shark.execution.Operator[_]]] = {
    //TODO: Currently only the top operators are used.  Instead we should
    // find an appropriate SelectOperator or FilterOperator.
    topOperators.map(op => (op, op.childOperators)).toMap
  }
  
  /** 
   * Make an RddScanOperator and insert it between @parent and @child.
   */
  def insertRddScanOperator(child: shark.execution.Operator[_], parent: shark.execution.Operator[_], inputRdd: RDD[_]): shark.execution.Operator[_] = {
    val newOp = new RddScanOperator()
    newOp.inputRdd = inputRdd
    val newHiveOp = new org.apache.hadoop.hive.ql.exec.ForwardOperator()
    newHiveOp.initializeCounters()
    newOp.hiveOp = newHiveOp
    insertOperatorBetween(child, newOp, parent)
    newOp
  }
  
  /**
   * Make an IntermediateCacheOperator and insert it between @parent and @child.
   */
  def insertCacheOperator(child: shark.execution.Operator[_], parent: shark.execution.Operator[_]): shark.execution.Operator[_] = {
    val newOp = new IntermediateCacheOperator()
    val newHiveOp = new org.apache.hadoop.hive.ql.exec.ForwardOperator()
    newHiveOp.initializeCounters()
    newOp.hiveOp = newHiveOp
    insertOperatorBetween(child, newOp, parent)
    newOp
  }
  
  private def insertOperatorBetween(oldChild: shark.execution.Operator[_], newOp: shark.execution.Operator[_], oldParent: shark.execution.Operator[_]): Unit = {
    oldChild.clearParents()
    oldParent.clearChildren()
    
    val childHiveOp = oldChild.hiveOp.asInstanceOf[org.apache.hadoop.hive.ql.exec.Operator[_ <: java.io.Serializable]]
    val newHiveOp = newOp.hiveOp.asInstanceOf[org.apache.hadoop.hive.ql.exec.Operator[_ <: java.io.Serializable]]
    val parentHiveOp = oldParent.hiveOp.asInstanceOf[org.apache.hadoop.hive.ql.exec.Operator[_ <: java.io.Serializable]]
    
    newHiveOp.setChildOperators(JavaConversions.seqAsJavaList(Seq(childHiveOp)))
    childHiveOp.setParentOperators(JavaConversions.seqAsJavaList(Seq(newHiveOp)))
    newHiveOp.setParentOperators(JavaConversions.seqAsJavaList(Seq(parentHiveOp)))
    parentHiveOp.setChildOperators(JavaConversions.seqAsJavaList(Seq(newHiveOp)))
    
    oldChild.addParent(newOp)
    oldParent.addChild(newOp)
  }
}