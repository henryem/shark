package blinkdb.experiment

import spark.SparkContext
import spark.RDD
import blinkdb.ErrorAnalysisConf
import blinkdb.util.MathUtils
import blinkdb.StandardDeviationErrorQuantifier
import blinkdb.DiagnosticOutput
import java.util.Random
import blinkdb.BootstrapConf
import blinkdb.ErrorQuantification
import edu.berkeley.blbspark.util.Statistics
import blinkdb.util.RddUtils

object ExperimentRunner {
  val FIXED_SEED = 012
  val DATA_GENERATOR: Random => Double = _.nextGaussian()
  
  def fetchData[D: ClassManifest](numRows: Int, generator: Random => D, sc: SparkContext, splits: Int, seed: Int): RDD[D] = {
    sc.parallelize(0 until numRows, splits)
      .mapPartitionsWithIndex({(partitionIdx, partition) =>
        val random = new Random(seed + partitionIdx)
        partition.map(_ => generator(random))
      })
  }
  
  def fetchLocalData[D](numRows: Int, generator: Random => D, seed: Int): Seq[D] = {
    val random = new Random(seed)
    (0 until numRows).map(_ => generator(random))
  }
  
  def main(args: Array[String]) {
    val master = args(0)
    val testType = args(1)
    val parallelism = args(2).toInt
    val numRows = args(3).toInt
    val test = testType match {
      case "singleJobDiagnostic" => doDiagnosticTest(master, parallelism, numRows, DiagnosticMethods.SingleJob)
      case "multipleJobDiagnostic" => doDiagnosticTest(master, parallelism, numRows, DiagnosticMethods.MultipleJob)
      case "singleJobBootstrap" => doBootstrapTest(master, parallelism, numRows, BootstrapMethods.SingleJob)
      case "multipleJobBootstrap" => doBootstrapTest(master, parallelism, numRows, BootstrapMethods.MultipleJob)
      case "broadcastBootstrap" => doBootstrapTest(master, parallelism, numRows, BootstrapMethods.Broadcast)
      case _ => require(false)
    }
    println(test)
  }
  
  def doDiagnosticTest(master: String, parallelism: Int, numRows: Int, method: DiagnosticMethod): DiagnosticOutput = {
    val sc = new SparkContext(master, "diagnosticExperiment")
    val data = fetchData(numRows, DATA_GENERATOR, sc, parallelism, FIXED_SEED)
    method match {
      case DiagnosticMethods.SingleJob =>
        SmallDiagnosticRunner.doSingleJobDiagnostic(
          data,
          Statistics.mean,
          StandardDeviationErrorQuantifier,
          ErrorAnalysisConf.default,
          sc,
          parallelism,
          FIXED_SEED
          )
      case DiagnosticMethods.MultipleJob =>
        SmallDiagnosticRunner.doMultipleJobDiagnostic(
          data,
          RddUtils.mean,
          StandardDeviationErrorQuantifier,
          ErrorAnalysisConf.default,
          sc,
          parallelism,
          FIXED_SEED
          )
    }
  }
  
  def doBootstrapTest(master: String, parallelism: Int, numRows: Int, method: BootstrapMethod): ErrorQuantification = {
    val sc = new SparkContext(master, "bootstrapExperiment")
    method match {
      case BootstrapMethods.Broadcast =>
        val data = fetchLocalData(numRows, DATA_GENERATOR, FIXED_SEED)
        SmallBootstrapRunner.doBroadcastBootstrap(
          data,
          Statistics.mean,
          StandardDeviationErrorQuantifier,
          BootstrapConf.default,
          sc,
          parallelism,
          FIXED_SEED
        )
      case BootstrapMethods.SingleJob =>
        val data = fetchData(numRows, DATA_GENERATOR, sc, parallelism, FIXED_SEED)
        SmallBootstrapRunner.doSingleJobBootstrap(
          data,
          {localData: Seq[Double] => (localData.size, localData.sum)},
          {(aggregateA: (Int, Double), aggregateB: (Int, Double)) => (aggregateA._1 + aggregateB._1, aggregateA._2 + aggregateB._2)},
          {aggregate: (Int, Double) => aggregate._2 / aggregate._1}, //NOTE: Together these 3 functions compute a mean.
          StandardDeviationErrorQuantifier,
          BootstrapConf.default,
          sc,
          parallelism,
          FIXED_SEED
        )
      case BootstrapMethods.MultipleJob =>
        val data = fetchData(numRows, DATA_GENERATOR, sc, parallelism, FIXED_SEED)
        SmallBootstrapRunner.doMultipleJobBootstrap(
          data,
          RddUtils.mean,
          StandardDeviationErrorQuantifier,
          BootstrapConf.default,
          sc,
          parallelism,
          FIXED_SEED
        )
    }
  }
  
  trait BootstrapMethod
  object BootstrapMethods {
    case object SingleJob extends BootstrapMethod
    case object MultipleJob extends BootstrapMethod
    case object Broadcast extends BootstrapMethod
  }
  
  trait DiagnosticMethod
  object DiagnosticMethods {
    case object SingleJob extends DiagnosticMethod
    case object MultipleJob extends DiagnosticMethod
  }
}