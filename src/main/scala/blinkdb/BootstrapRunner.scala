package blinkdb

import edu.berkeley.blbspark.StratifiedBlb
import shark.execution.SparkTask
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import shark.parse.QueryContext
import spark.RDD
import shark.execution.HiveOperator
import shark.execution.SparkWork
import edu.berkeley.blbspark.WeightedItem
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.execution.serialization.KryoSerializer
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import shark.SharkEnv
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import shark.LogHelper
import blinkdb.util.HiveUtils
import blinkdb.util.CollectionUtils._
import blinkdb.parse.InputExtractionSemanticAnalyzer
import blinkdb.util.ResampleGenerator
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

object BootstrapRunner extends LogHelper {
  private val NUM_BOOTSTRAP_RESAMPLES = 20 //TODO: 100?
  
  def doBootstrap[E <: ErrorQuantification](
      cmd: String,
      inputRdd: RDD[Any],
      errorQuantifier: ErrorQuantifier[E],
      conf: HiveConf,
      seed: Int)
      (implicit ec: ExecutionContext):
      Future[Seq[Seq[E]]] = {
    //FIXME: Use a random seed.
    val resampleRdds = ResampleGenerator.generateResamples(inputRdd, BootstrapRunner.NUM_BOOTSTRAP_RESAMPLES, seed)
    val resultRdds = resampleRdds.map({ resampleRdd => 
      //TODO: Reuse semantic analysis across runs.  For now this avoids the
      // hassle of reaching into the graph and replacing the resample RDD,
      // and it also avoids any bugs that might result from executing an
      // operator graph more than once.
      val sem = QueryRunner.doSemanticAnalysis(cmd, BootstrapStage.BootstrapExecution, conf, Some(resampleRdd))
      QueryRunner.executeOperatorTree(sem)
    })
    val bootstrapOutputsFuture = QueryRunner.collectQueryOutputs(resultRdds)
    bootstrapOutputsFuture.map(bootstrapOutputs => {
      errorQuantifier.computeError(bootstrapOutputs)
    })
  }
}
