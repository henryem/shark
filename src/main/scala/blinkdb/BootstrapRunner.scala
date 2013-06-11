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
import edu.berkeley.blbspark.ResampleGenerator
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import blinkdb.util.LoggingUtils
import akka.dispatch.Futures

object BootstrapRunner extends LogHelper {
  def doBootstrap[E <: ErrorQuantification, B <: QueryExecutionBuilder[B]](
      queryBuilder: B,
      inputRdd: RDD[Any],
      errorQuantifier: ErrorQuantifier[E],
      bootstrapConf: BootstrapConf,
      seed: Int)
      (implicit ec: ExecutionContext):
      Future[Seq[Seq[E]]] = {
    val resampleTimer = LoggingUtils.startCount("Creating resample input RDDs")
    val resampleRdds = ResampleGenerator.generateResamples(inputRdd, bootstrapConf.numBootstrapResamples, seed)
    resampleTimer.stop()
    //NOTE: The validity of this timing number relies on resampleRdds being an
    // eager collection.  There is no point in it being lazy, so this isn't a
    // big deal.
    val query = queryBuilder.forStage(ErrorAnalysisStage.BootstrapExecution).build()
    val queryCreationTimer = LoggingUtils.startCount("Creating resample queries and forming output RDDs")
    val resampleOutputsFuture = Futures.sequence(resampleRdds.map({ resampleRdd => query.execute(resampleRdd)(ec) }), ec)
    queryCreationTimer.stop()
    resampleOutputsFuture.map(resampleOutputs => {
      errorQuantifier.computeError(resampleOutputs.toSeq)
    })
  }
}
