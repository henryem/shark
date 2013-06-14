package shark.execution

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSuite
import shark.execution.{Operator => SharkOperator, OperatorFactory => SharkOperatorFactory}
import org.apache.hadoop.hive.ql.exec.{
  Operator => HOperator,
  FileSinkOperator => HFileSinkOperator,
  SelectOperator => HSelectOperator,
  TableScanOperator => HTableScanOperator,
  OperatorFactory => HOperatorFactory}
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.hadoop.hive.ql.plan.SelectDesc
import org.apache.hadoop.hive.ql.plan.TableScanDesc
import java.util.ArrayList
import com.google.common.collect.Lists

class OperatorSuite extends FunSuite with ShouldMatchers {
  import OperatorSuite._
  test("Operator.returnTerminalOperators()") {
    val graph = makeExampleSharkOperatorGraph()
    graph(0).returnTerminalOperators() should equal (Seq(graph(0)))
    graph(1).returnTerminalOperators() should equal (Seq(graph(0)))
    graph(2).returnTerminalOperators() should equal (Seq(graph(0)))
  }
  
  test("Operator.returnTopOperators()") {
    val graph = makeExampleSharkOperatorGraph()
    graph(0).returnTopOperators() should equal (Seq(graph(2)))
    graph(1).returnTopOperators() should equal (Seq(graph(2)))
    graph(2).returnTopOperators() should equal (Seq(graph(2)))
  }
  
  test("Operator.findOpsBelowMatching()") {
    val graph = makeExampleSharkOperatorGraph()
    graph(0).findOpsBelowMatching(_ => true).toSet should equal (Seq(graph(0)).toSet)
    graph(1).findOpsBelowMatching(_ => true).toSet should equal (Seq(graph(0), graph(1)).toSet)
    graph(2).findOpsBelowMatching(_ => true).toSet should equal (Seq(graph(0), graph(1), graph(2)).toSet)
    
    graph(0).findOpsBelowMatching(op => op.isInstanceOf[SelectOperator]).toSet should equal (Seq().toSet)
    graph(1).findOpsBelowMatching(op => op.isInstanceOf[SelectOperator]).toSet should equal (Seq(graph(1)).toSet)
    graph(2).findOpsBelowMatching(op => op.isInstanceOf[SelectOperator]).toSet should equal (Seq(graph(1)).toSet)
  }
  
  test("Operator.findOpsAboveMatching()") {
    val graph = makeExampleSharkOperatorGraph()
    graph(0).findOpsAboveMatching(_ => true).toSet should equal (Seq(graph(0), graph(1), graph(2)).toSet)
    graph(1).findOpsAboveMatching(_ => true).toSet should equal (Seq(graph(1), graph(2)).toSet)
    graph(2).findOpsAboveMatching(_ => true).toSet should equal (Seq(graph(2)).toSet)
    
    graph(0).findOpsAboveMatching(op => op.isInstanceOf[SelectOperator]).toSet should equal (Seq(graph(1)).toSet)
    graph(1).findOpsAboveMatching(op => op.isInstanceOf[SelectOperator]).toSet should equal (Seq(graph(1)).toSet)
    graph(2).findOpsAboveMatching(op => op.isInstanceOf[SelectOperator]).toSet should equal (Seq().toSet)
  }
}

object OperatorSuite {
  // Returns a simple 3-node graph, terminal node first.
  private def makeExampleHiveOperatorGraph(): Seq[HOperator[_]] = {
    val bottom = HOperatorFactory.get(classOf[FileSinkDesc])
    val middle = HOperatorFactory.get(classOf[SelectDesc])
    middle.setChildOperators(Lists.newArrayList(bottom))
    bottom.setParentOperators(Lists.newArrayList(middle))
    val top = HOperatorFactory.get(classOf[TableScanDesc])
    top.setChildOperators(Lists.newArrayList(middle))
    middle.setParentOperators(Lists.newArrayList(top))
    Seq(bottom, middle, top)
  }
  
  // Returns a simple 3-node graph, terminal node first.
  private def makeExampleSharkOperatorGraph(): Seq[SharkOperator[_ <: HiveOperator]] = {
    val hiveGraph = makeExampleHiveOperatorGraph()
    val sharkGraph = SharkOperatorFactory.createSharkPlan(hiveGraph(0))
    Seq(
        sharkGraph,
        sharkGraph.parentOperators(0).asInstanceOf[SharkOperator[_ <: HiveOperator]],
        sharkGraph.parentOperators(0).parentOperators(0).asInstanceOf[SharkOperator[_ <: HiveOperator]])
  }
}