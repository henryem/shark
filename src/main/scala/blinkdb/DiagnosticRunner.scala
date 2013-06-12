package blinkdb
import spark.RDD
import org.apache.hadoop.hive.conf.HiveConf
import shark.SharkEnv
import blinkdb.util.CollectionUtils._
import blinkdb.ErrorQuantifications._
import shark.LogHelper
import edu.berkeley.blbspark.ResampleGenerator
import shark.execution.RddCacheHelper
import java.util.Random
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import edu.berkeley.blbspark.util.Statistics
import spark.SparkContext._
import spark.Partitioner
import blinkdb.util.RddUtils
import blinkdb.util.FutureRddOps._
import java.util.concurrent.Executors
import akka.dispatch.Await
import spark.LocalSpark
import akka.util.Duration
import akka.dispatch.Futures
import blinkdb.util.MathUtils
import blinkdb.util.LoggingUtils

object DiagnosticRunner extends LogHelper {
  private val NUM_DIAGNOSTIC_SUBSAMPLES = 10 //TODO: 100?
  
  //FIXME: Make this parametric.
  private val DIAGNOSTIC_SUBSAMPLE_SIZES = Seq(100, 500, 1000)

  // An acceptable level of deviation between the average bootstrap output
  // and ground truth, normalized by ground truth.  This is c_1 in the
  // diagnostics paper.
  private val ACCEPTABLE_RELATIVE_MEAN_DEVIATION = .2 //FIXME
  
  // An acceptable level of standard deviation among bootstrap outputs,
  // normalized by ground truth.  This is c_2 in the diagnostics paper.
  private val ACCEPTABLE_RELATIVE_STANDARD_DEVIATION = .2 //FIXME
  
  // An acceptable proportion of bootstrap outputs that are within
  // ACCEPTABLE_DEVIATION of ground truth.  This is \alpha in the diagnostics
  // paper.
  private val ACCEPTABLE_PROPORTION_NEAR_GROUND_TRUTH = .95
  // See ACCEPTABLE_PROPORTION_NEAR_GROUND_TRUTH above.  This is c_3 in the
  // diagnostics paper.
  private val ACCEPTABLE_DEVIATION = .2
  
  /**
   * @param inputRdd must be cached.
   */
  def doDiagnostic[E <: ErrorQuantification, B <: QueryExecutionBuilder[B]](
      queryBuilder: B,
      inputRdd: RDD[Any],
      inputRddSize: Long,
      errorQuantifier: ErrorQuantifier[E],
      errorAnalysisConf: ErrorAnalysisConf,
      seed: Int)
      (implicit ec: ExecutionContext):
      Future[DiagnosticOutput] = {
    val diagnosticConf = errorAnalysisConf.diagnosticConf
    val parallelism = inputRdd.partitions.size
    
//    println("Shuffling input.") //TMP
//    val preshuffleTimer = LoggingUtils.startCount("Shuffling input as a precursor to diagnosis.")
//    //TODO: This is expensive.  We can instead shuffle only a part of the
//    // input (since only diagnosticSubsampleSizes.sum*numDiagnosticSubsamples
//    // rows are actually needed), or else assume that it has been shuffled
//    // ahead of time.
//    val shuffledInputRdd = RddUtils.randomlyPermute(inputRdd, new Random(seed).nextInt)
//    shuffledInputRdd.foreach(_ => Unit) //TMP
//    preshuffleTimer.stop()
//    println("Done shuffling input.  Some rows: %s".format(shuffledInputRdd.take(15).deep.toString))
    val shuffledInputRdd = inputRdd
    
    //TODO: Divide subsamples of different sizes, to allow for parallelism up
    // to numDiagnosticSubsamples*diagnosticSubsampleSizes.size.
    println("Parallelism: %d".format(parallelism)) //TMP
    val partitionToSubsampleSizes: Int => Seq[Int] = MathUtils.divideAmong(diagnosticConf.numDiagnosticSubsamples, parallelism)
      .map(numSubsamples => Seq.fill(numSubsamples)(diagnosticConf.diagnosticSubsampleSizes).flatten)
    val diagnosticResultsFuture: Future[Seq[(Int, (SingleQueryIterateOutput, Seq[Seq[E]]))]] = shuffledInputRdd
      .mapPartitionsWithIndex((partitionIdx, partition) => {
        println("Computing diagnostic on partition %d".format(partitionIdx)) //TMP
        val random = new Random(seed + partitionIdx)
        val subsampleSizes = partitionToSubsampleSizes(partitionIdx)
        val query = queryBuilder.forStage(ErrorAnalysisStage.DiagnosticExecution).build()
        val subsampleResults: Seq[(Int, (SingleQueryIterateOutput, Seq[Seq[E]]))] = Await.result(
          LocalSpark.runInLocalContext({sc =>
            println("Running in local context") //TMP
            subsampleSizes.map(subsampleSize => {
              println("Computing diagnostic subsample of size %d".format(subsampleSize)) //TMP
              val executor = Executors.newSingleThreadExecutor()
              val ec = ExecutionContext.fromExecutor(executor)
              val subsample = partition.take(subsampleSize).toSeq
              val queryOutput = query.executeNow(LocalSpark.createLocalRdd(subsample, sc))(ec)
              //TODO: Use BootstrapRunner here instead.
              val bootstrapResamples = (0 until errorAnalysisConf.bootstrapConf.numBootstrapResamples)
                .map(idx => ResampleGenerator.generateLocalResample(subsample, random.nextInt, true))
                .map(resample => query.executeNow(LocalSpark.createLocalRdd(resample, sc))(ec))
              val bootstrapResult = errorQuantifier.computeError(bootstrapResamples)
              (subsampleSize, (queryOutput, bootstrapResult))
            })
          }),
          Duration.Inf)
        subsampleResults.iterator
      })
      .collectFuture
      .map(_.toSeq)
    
    println("Done producing futures.") //TMP
    // Once all diagnostic results for individual subsamples are in, group by
    // subsample size and decide whether the diagnostic passes.
    diagnosticResultsFuture.map(diagnosticResults => {
      println("Examining diagnostic results locally.") //TMP
      val resultsBySubsampleSize: Map[Int, Seq[(SingleQueryIterateOutput, Seq[Seq[E]])]] = diagnosticResults
        .groupBy(_._1)
        .mapValues(_.map(_._2))
      val diagnosticsPerSubsampleSize = resultsBySubsampleSize
        .map({ case (subsampleSize, results) =>
          val queryOutputs = results.map(_._1)
          val groundTruthEstimate = errorQuantifier.computeError(queryOutputs)
          val bootstrapOutputs = results.map(_._2)
          DiagnosticRunner.computeSingleDiagnosticResult(subsampleSize, groundTruthEstimate, bootstrapOutputs, diagnosticConf)
        })
        .toSeq
      DiagnosticRunner.computeFinalDiagnostic(diagnosticsPerSubsampleSize, diagnosticConf)
    })
  }
  
  def computeSingleDiagnosticResult[E <: ErrorQuantification](subsampleSize: Int, groundTruth: Seq[Seq[E]], bootstrapOutputs: Seq[Seq[Seq[E]]], diagnosticConf: DiagnosticConf): SingleDiagnosticResult = {
    val relDev: Seq[Seq[Double]] = computeRelativeDeviation(groundTruth, bootstrapOutputs)
    val relStdDev: Seq[Seq[Double]] = computeNormalizedStandardDeviation(groundTruth, bootstrapOutputs)
    val proportionNearGroundTruth: Seq[Seq[Double]] = computeProportionNearGroundTruth(groundTruth, bootstrapOutputs, diagnosticConf)
    SingleDiagnosticResult(subsampleSize, relDev, relStdDev, proportionNearGroundTruth)
  }
  
  def computeFinalDiagnostic(resultsPerSubsampleSize: Seq[SingleDiagnosticResult], diagnosticConf: DiagnosticConf): DiagnosticOutput = {
    println("Diagnostic results per subsample size: %s".format(resultsPerSubsampleSize)) //TMP
      val areRelDevsAcceptable = areRelativeDeviationsAcceptable(resultsPerSubsampleSize, diagnosticConf)
      val areRelStdDevsAcceptable = areRelativeStandardDeviationsAcceptable(resultsPerSubsampleSize, diagnosticConf)
      val arePropsNearGroundTruthAcceptable = areProportionsNearGroundTruthAcceptable(resultsPerSubsampleSize, diagnosticConf)
      println("areRelDevsAcceptable: %b, areRelStdDevsAcceptable: %b, areProportionsNearGroundTruthAcceptable: %b".format(areRelDevsAcceptable, areRelStdDevsAcceptable, arePropsNearGroundTruthAcceptable)) //TMP
      
      DiagnosticOutput(
          areRelDevsAcceptable
          && areRelStdDevsAcceptable
          && arePropsNearGroundTruthAcceptable)
  }
  
  private def areRelativeDeviationsAcceptable(resultsPerSubsampleSize: Seq[SingleDiagnosticResult], conf: DiagnosticConf): Boolean = {
    resultsPerSubsampleSize
      .map(_.relDev)
      .toSeq
      .aggregateNested(_.sliding(2).map(slidingWindow => slidingWindow(1) < slidingWindow(0) || slidingWindow(1) <= conf.acceptableRelativeMeanDeviation).forall(identity))
      .forall(_.forall(identity))
  }
  
  private def computeRelativeDeviation[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]]): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .aggregateNested(Statistics.mean)
      .zipNested(groundTruth.mapNested(_.toDouble))
      .mapNested({ case (bootstrapAverage, groundTruthResult) => (bootstrapAverage - groundTruthResult) / groundTruthResult }) //TODO: Handle div-by-zero
      .mapNested(math.abs)
  }
  
  private def areRelativeStandardDeviationsAcceptable(resultsPerSubsampleSize: Seq[SingleDiagnosticResult], conf: DiagnosticConf): Boolean = {
    resultsPerSubsampleSize
      .map(_.relStdDev)
      .toSeq
      .aggregateNested(_.sliding(2).map(slidingWindow => slidingWindow(1) < slidingWindow(0) || slidingWindow(1) <= conf.acceptableRelativeStandardDeviation).forall(identity))
      .forall(_.forall(identity))
  }
  
  private def computeNormalizedStandardDeviation[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]]): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .aggregateNested(StandardDeviationErrorQuantifier.standardDeviation)
      .zipNested(groundTruth.mapNested(_.toDouble))
      .mapNested({ case (bootstrapStdDev, groundTruthResult) => bootstrapStdDev / groundTruthResult }) //TODO: Handle div-by-zero
  }
  
  private def areProportionsNearGroundTruthAcceptable(resultsPerSubsampleSize: Seq[SingleDiagnosticResult], conf: DiagnosticConf): Boolean = {
    resultsPerSubsampleSize
      .max(Ordering.by[SingleDiagnosticResult, Int](_.subsampleSize))
      .proportionNearGroundTruth
      .forall(_.forall(_ >= conf.acceptableProportionNearGroundTruth))
  }
  
  private def computeProportionNearGroundTruth[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]], conf: DiagnosticConf): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .map(_.zipNested(groundTruth.mapNested(_.toDouble)))
      .map(_.mapNested({ case (bootstrapOutput, groundTruthResult) => math.abs((groundTruthResult - bootstrapOutput) / groundTruthResult) }))
      .map(_.mapNested(relativeDeviation => if (relativeDeviation <= conf.acceptableGroundTruthDeviation) 1.0 else 0.0))
      .aggregateNested(Statistics.mean)
  }
}

case class SingleDiagnosticResult(subsampleSize: Int, relDev: Seq[Seq[Double]], relStdDev: Seq[Seq[Double]], proportionNearGroundTruth: Seq[Seq[Double]])