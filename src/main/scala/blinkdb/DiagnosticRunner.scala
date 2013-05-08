package blinkdb
import spark.RDD
import org.apache.hadoop.hive.conf.HiveConf
import shark.SharkEnv
import blinkdb.util.CollectionUtils._
import blinkdb.ErrorQuantifications._
import shark.LogHelper
import blinkdb.util.ResampleGenerator
import shark.execution.RddCacheHelper
import java.util.Random
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import edu.berkeley.blbspark.util.Statistics

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
  def doDiagnostic[E <: ErrorQuantification](
      cmd: String,
      inputRdd: RDD[Any],
      inputRddSize: Long,
      errorQuantifier: ErrorQuantifier[E],
      conf: HiveConf,
      seed: Int)
      (implicit ec: ExecutionContext):
      Future[DiagnosticOutput] = {
    val random = new Random(seed)
    //TODO: This is a little hard to read.
    val resultsPerSubsampleSizeFuture: Future[Seq[SingleDiagnosticResult]] = Future.sequence(DIAGNOSTIC_SUBSAMPLE_SIZES.map({ subsampleSize =>
      val subsampleRdds = ResampleGenerator.generateSubsamples(inputRdd, NUM_DIAGNOSTIC_SUBSAMPLES, subsampleSize, inputRddSize, random.nextInt)
      val resultRddsAndBootstrapOutputs = subsampleRdds.map({ subsampleRdd =>
        //TODO: Reuse semantic analysis across runs.  For now this avoids the
        // hassle of reaching into the graph and replacing the resample RDD,
        // and it also avoids any bugs that might result from executing an
        // operator graph more than once.
        val sem = QueryRunner.doSemanticAnalysis(cmd, ErrorAnalysisStage.DiagnosticExecution, conf, Some(subsampleRdd))
        //TODO: Restrict Shark to use only a single partition for this subsample.
        val (trueQueryOutputRdd, objectInspector) = QueryRunner.executeOperatorTree(sem)
        val trueQueryOutputFuture = QueryRunner.collectSingleQueryOutput(trueQueryOutputRdd, objectInspector)
        val bootstrapOutputFuture = BootstrapRunner.doBootstrap(cmd, subsampleRdd, errorQuantifier, conf, random.nextInt)
        (trueQueryOutputFuture, bootstrapOutputFuture)
      })
      val subsamplingOutputsFuture = Future.sequence(resultRddsAndBootstrapOutputs.map(_._1))
      val groundTruthFuture = subsamplingOutputsFuture.map(subsamplingOutputs => errorQuantifier.computeError(subsamplingOutputs))
      val subsamplingBootstrapOutputsFuture = Future.sequence(resultRddsAndBootstrapOutputs.map(_._2))
      groundTruthFuture
        .zip(subsamplingBootstrapOutputsFuture)
        .map({case (groundTruth, subsamplingBootstrapOutputs) => 
          val relDev: Seq[Seq[Double]] = computeRelativeDeviation(groundTruth, subsamplingBootstrapOutputs)
          val relStdDev: Seq[Seq[Double]] = computeNormalizedStandardDeviation(groundTruth, subsamplingBootstrapOutputs)
          val proportionNearGroundTruth: Seq[Seq[Double]] = computeProportionNearGroundTruth(groundTruth, subsamplingBootstrapOutputs)
          SingleDiagnosticResult(subsampleSize, relDev, relStdDev, proportionNearGroundTruth)
        })
    }))
    resultsPerSubsampleSizeFuture.map(resultsPerSubsampleSize => {
      println("Diagnostic results per subsample size: %s".format(resultsPerSubsampleSize)) //TMP
      val areRelDevsAcceptable = areRelativeDeviationsAcceptable(resultsPerSubsampleSize)
      val areRelStdDevsAcceptable = areRelativeStandardDeviationsAcceptable(resultsPerSubsampleSize)
      val arePropsNearGroundTruthAcceptable = areProportionsNearGroundTruthAcceptable(resultsPerSubsampleSize)
      println("areRelDevsAcceptable: %b, areRelStdDevsAcceptable: %b, areProportionsNearGroundTruthAcceptable: %b".format(areRelDevsAcceptable, areRelStdDevsAcceptable, arePropsNearGroundTruthAcceptable)) //TMP
      
      DiagnosticOutput(
          areRelDevsAcceptable
          && areRelStdDevsAcceptable
          && arePropsNearGroundTruthAcceptable)
    })
  }
  
  private def areRelativeDeviationsAcceptable(resultsPerSubsampleSize: Seq[SingleDiagnosticResult]): Boolean = {
    resultsPerSubsampleSize
      .map(_.relDev)
      .toSeq
      .aggregateNested(_.sliding(2).map(slidingWindow => slidingWindow(1) < slidingWindow(0) || slidingWindow(1) <= ACCEPTABLE_RELATIVE_MEAN_DEVIATION).forall(identity))
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
  
  private def areRelativeStandardDeviationsAcceptable(resultsPerSubsampleSize: Seq[SingleDiagnosticResult]): Boolean = {
    resultsPerSubsampleSize
      .map(_.relStdDev)
      .toSeq
      .aggregateNested(_.sliding(2).map(slidingWindow => slidingWindow(1) < slidingWindow(0) || slidingWindow(1) <= ACCEPTABLE_RELATIVE_STANDARD_DEVIATION).forall(identity))
      .forall(_.forall(identity))
  }
  
  private def computeNormalizedStandardDeviation[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]]): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .aggregateNested(StandardDeviationErrorQuantifier.standardDeviation)
      .zipNested(groundTruth.mapNested(_.toDouble))
      .mapNested({ case (bootstrapStdDev, groundTruthResult) => bootstrapStdDev / groundTruthResult }) //TODO: Handle div-by-zero
  }
  
  private def areProportionsNearGroundTruthAcceptable(resultsPerSubsampleSize: Seq[SingleDiagnosticResult]): Boolean = {
    resultsPerSubsampleSize
      .max(Ordering.by[SingleDiagnosticResult, Int](_.subsampleSize))
      .proportionNearGroundTruth
      .forall(_.forall(_ >= ACCEPTABLE_PROPORTION_NEAR_GROUND_TRUTH))
  }
  
  private def computeProportionNearGroundTruth[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]]): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .map(_.zipNested(groundTruth.mapNested(_.toDouble)))
      .map(_.mapNested({ case (bootstrapOutput, groundTruthResult) => math.abs((groundTruthResult - bootstrapOutput) / groundTruthResult) }))
      .map(_.mapNested(relativeDeviation => if (relativeDeviation <= ACCEPTABLE_DEVIATION) 1.0 else 0.0))
      .aggregateNested(Statistics.mean)
  }
}

case class SingleDiagnosticResult(subsampleSize: Int, relDev: Seq[Seq[Double]], relStdDev: Seq[Seq[Double]], proportionNearGroundTruth: Seq[Seq[Double]])