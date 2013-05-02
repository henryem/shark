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

object DiagnosticRunner extends LogHelper {
  private val NUM_DIAGNOSTIC_SUBSAMPLES = 5 //TODO: 100?
  
  //FIXME: Make this parametric.
  private val DIAGNOSTIC_SUBSAMPLE_SIZES = Seq(100, 500)

  // An acceptable level of deviation between the average bootstrap output
  // and ground truth, normalized by ground truth.  This is c_1 in the
  // diagnostics paper.
  private val ACCEPTABLE_RELATIVE_MEAN_DEVIATION = .5 //FIXME
  
  // An acceptable level of standard deviation among bootstrap outputs,
  // normalized by ground truth.  This is c_2 in the diagnostics paper.
  private val ACCEPTABLE_RELATIVE_STANDARD_DEVIATION = .5 //FIXME
  
  def doDiagnostic[E <: ErrorQuantification](
      cmd: String,
      inputRdd: RDD[Any],
      inputRddCacher: RddCacheHelper,
      errorQuantifier: ErrorQuantifier[E],
      conf: HiveConf,
      seed: Int):
      DiagnosticOutput = {
    //TODO: Implement diagnostic step 3.
    val random = new Random(seed)
    val resultsPerSubsampleSize = DIAGNOSTIC_SUBSAMPLE_SIZES.map({ subsampleSize =>
      println("Running diagnostic for subsample size %d.".format(subsampleSize)) //TMP
      //TMP
      val subsampleRdds = ResampleGenerator.generateSubsamples(inputRdd, inputRddCacher, NUM_DIAGNOSTIC_SUBSAMPLES, subsampleSize, random.nextInt)
      val resultRddsAndBootstrapOutputs = subsampleRdds.map({ subsampleRdd => 
        println("Running a diagnostic iteration.") //TMP
        //TODO: Reuse semantic analysis across runs.  For now this avoids the
        // hassle of reaching into the graph and replacing the resample RDD,
        // and it also avoids any bugs that might result from executing an
        // operator graph more than once.
        val sem = QueryRunner.doSemanticAnalysis(cmd, BootstrapStage.DiagnosticExecution, conf, Some(subsampleRdd))
        //TODO: Restrict Shark to use only a single partition for this subsample.
        val trueQueryOutput = QueryRunner.executeOperatorTree(sem)
        //TODO: Just get the bootstrap RDDs and execute them all in a batch
        // later.  Executing them sequentially will probably kill performance.
        val bootstrapOutput = BootstrapRunner.doBootstrap(cmd, subsampleRdd, errorQuantifier, conf, random.nextInt)
        println("Bootstrap output for diagnostic iteration: %s".format(bootstrapOutput)) 
        (trueQueryOutput, bootstrapOutput)
      })
      //TODO: Rename the function to something more generic and use it to
      // collect everything at the same time.
      val subsamplingOutputs = QueryRunner.collectQueryOutputs(resultRddsAndBootstrapOutputs.map(_._1))
      println("Diagnostic ground truth outputs: %s".format(subsamplingOutputs)) //TMP
      val groundTruth = errorQuantifier.computeError(subsamplingOutputs)
      println("Ground truth: %s".format(groundTruth)) //TMP
      val subsamplingBootstrapOutputs = resultRddsAndBootstrapOutputs.map(_._2)
      
      val relDev: Seq[Seq[Double]] = computeRelativeDeviation(groundTruth, subsamplingBootstrapOutputs)
      val relStdDev: Seq[Seq[Double]] = computeNormalizedStandardDeviation(groundTruth, subsamplingBootstrapOutputs)
      SingleDiagnosticResult(relDev, relStdDev)
    })
    println(resultsPerSubsampleSize) //TMP
    val areRelDevsAcceptable = resultsPerSubsampleSize
      .map(_.relDev)
      .toSeq
      .aggregateNested(_.sliding(2).map(slidingWindow => slidingWindow(1) < slidingWindow(0) || slidingWindow(1) <= ACCEPTABLE_RELATIVE_MEAN_DEVIATION).forall(identity))
      .forall(_.forall(identity))
    val areRelStdDevsAcceptable = resultsPerSubsampleSize
      .map(_.relStdDev)
      .toSeq
      .aggregateNested(_.sliding(2).map(slidingWindow => slidingWindow(1) < slidingWindow(0) || slidingWindow(1) <= ACCEPTABLE_RELATIVE_STANDARD_DEVIATION).forall(identity))
      .forall(_.forall(identity))
    DiagnosticOutput(areRelDevsAcceptable && areRelStdDevsAcceptable)
  }
  
  private def computeRelativeDeviation[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]]): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .reduceNested(_ + _) //TODO: This is confusing.  Write a function to just compute the mean.
      .mapNested(_ / subsamplingBootstrapOutputs.size.toDouble)
      .zipNested(groundTruth.mapNested(_.toDouble))
      .mapNested({ case (bootstrapAverage, groundTruthResult) => (bootstrapAverage - groundTruthResult) / groundTruthResult }) //TODO: Handle div-by-zero
      .mapNested(math.abs)
  }
  
  private def computeNormalizedStandardDeviation[E <: ErrorQuantification](groundTruth: Seq[Seq[E]], subsamplingBootstrapOutputs: Seq[Seq[Seq[E]]]): Seq[Seq[Double]] = {
    subsamplingBootstrapOutputs
      .map(_.mapNested(_.toDouble))
      .aggregateNested(StandardDeviationErrorQuantifier.standardDeviation)
      .zipNested(groundTruth.mapNested(_.toDouble))
      .mapNested({ case (bootstrapStdDev, groundTruthResult) => bootstrapStdDev / groundTruthResult }) //TODO: Handle div-by-zero
  }
}

case class SingleDiagnosticResult(relDev: Seq[Seq[Double]], relStdDev: Seq[Seq[Double]])