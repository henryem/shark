package blinkdb.parse

import scala.collection.JavaConversions
import org.apache.hadoop.hive.conf.HiveConf
import shark.execution.HiveOperator
import shark.execution.IntermediateCacheOperator
import shark.execution.RddScanOperator
import shark.execution.TerminalOperator
import spark.RDD
import shark.parse.SharkSemanticAnalyzer
import shark.execution.OperatorFactory

class InputExtractionSemanticAnalyzer(conf: HiveConf) extends SharkSemanticAnalyzer(conf) {
  //HACK: This should be part of a proper API.
  var intermediateInputOperator: shark.execution.IntermediateCacheOperator = _
  
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
    val intermediateCacheOp = BlinkDbSemanticAnalyzers.insertCacheOperator(child, parent)
    this.intermediateInputOperator = intermediateCacheOp
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
  
  override def createOutputPlan(hiveOp: HiveOperator): Option[TerminalOperator] = {
    // We always use a TableRddSinkOperator.  The usual FileSinkOperator will
    // collect the result RDD before returning it, which is undesirable for
    // performance reasons.
    Some(OperatorFactory.createSharkRddOutputPlan(hiveOp))
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
   * ( MemoryStoreSinkOperator )
   * 
   * In this example, this method will return a map from the SelectOperator to
   * a singleton list containing the GroupByOperator.
   */
  def getPostInputScanOperators(topOperators: Seq[shark.execution.Operator[_ <: HiveOperator]]): Map[shark.execution.Operator[_], Seq[shark.execution.Operator[_]]] = {
    //TODO: Currently only the top operators are used.  Instead we should
    // find an appropriate SelectOperator or FilterOperator.
    //TODO: Support non-linear graphs.
    //TODO: Clarify what a "post-input scan operator" is.
    val cutOperators: Seq[shark.execution.Operator[_ <: HiveOperator]] = topOperators.flatMap(op => getPostInputScanOperators(op)).distinct
    val cut: Seq[(shark.execution.Operator[_], Seq[shark.execution.Operator[_]])] = cutOperators.map(op => (op, op.childOperators))
    cut.toMap
  }
  
  // Get all post-input scan operators below @operator.
  private def getPostInputScanOperators(operator: shark.execution.Operator[_ <: HiveOperator]): Seq[shark.execution.Operator[_ <: HiveOperator]] = {
    if (! operator.childOperators.map(child => isOneToSomeOperator(child)).forall(identity)) {
      List(operator)
    } else {
      //HACK: Need to figure out how to remove this cast - it's ugly, though harmless.
      operator.childOperators
        .flatMap(op => getPostInputScanOperators(op.asInstanceOf[shark.execution.Operator[_ <: HiveOperator]]))
        .distinct
    }
  }
  
  // True if @operator scans a table or deterministically maps each input row
  // to 0 or 1 outputs.  For example, projection and filter operators that
  // evaluate deterministic functions satisfy this.  Group-by operators,
  // which map several rows to a single row, do not.
  private def isOneToSomeOperator(operator: shark.execution.Operator[_]): Boolean = {
    //TODO: Not sure if there should be more operators in this list, or if
    // there is an automatic way to implement this.
    operator.isInstanceOf[shark.execution.TableScanOperator] ||
      operator.isInstanceOf[shark.execution.SelectOperator] ||
      operator.isInstanceOf[shark.execution.FilterOperator] ||
      operator.isInstanceOf[shark.execution.ForwardOperator]
  }
  
  /** 
   * Make an RddScanOperator and insert it between @parent and @child.
   */
  def insertRddScanOperator(child: shark.execution.Operator[_], parent: shark.execution.Operator[_], inputRdd: RDD[_]): RddScanOperator = {
    val newOp = new RddScanOperator()
    newOp.inputRdd = inputRdd
    val newHiveOp = RddScanOperator.makePartnerHiveOperator()
    newHiveOp.initializeCounters()
    newOp.hiveOp = newHiveOp
    insertOperatorBetween(child, newOp, parent)
    newOp
  }
  
  /**
   * Make an IntermediateCacheOperator and insert it between @parent and @child.
   */
  def insertCacheOperator(child: shark.execution.Operator[_], parent: shark.execution.Operator[_]): IntermediateCacheOperator = {
    val newOp = new IntermediateCacheOperator()
    //TODO: Shouldn't need to make this Hive Operator here - move it to a
    // static factory in IntermediateCacheOperator.
    val newHiveOp = IntermediateCacheOperator.makePartnerHiveOperator()
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