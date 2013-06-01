package blinkdb.experiment
import blinkdb.ErrorQuantifier
import spark.SparkContext
import edu.berkeley.blbspark.ResampleGenerator
import blinkdb.DiagnosticOutput
import blinkdb.util.LoggingUtils
import org.apache.hadoop.io.NullWritable
import edu.berkeley.blbspark.dist.BernoulliDistribution
import java.util.Random
import spark.SparkContext._
import spark.Partitioner
import blinkdb.DiagnosticRunner
import blinkdb.DiagnosticConf
import blinkdb.ErrorQuantification
import spark.RDD

object SmallBootstrapRunner {
  val DEFAULT_NUM_BOOTSTRAP_RESAMPLES = 100
  
  def doBroadcastBootstrap[D,E <: ErrorQuantification](
      data: Seq[D],
      queryFunc: Seq[D] => Double,
      errorQuantifier: ErrorQuantifier[E],
      numResamples: Int,
      sc: SparkContext,
      numSplits: Int,
      seed: Int): E = {
    val broadcastTimer = LoggingUtils.startCount("Broadcasting the data")
    val dataBroadcast = sc.broadcast(data)
    broadcastTimer.stop
    val resampleIndicesParallelizeTimer = LoggingUtils.startCount("Parallelizing %d indices".format(numResamples))
    val resampleIndices = sc.parallelize(0 until numResamples, numSplits)
    resampleIndicesParallelizeTimer.stop
    val bootstrapTimer = LoggingUtils.startCount("Running %d bootstraps".format(numResamples))
    val bootstrapIterateResults: Seq[Double] = resampleIndices
      .map(idx => ResampleGenerator.generateLocalResample(dataBroadcast.value, seed + idx, true))
      .map(queryFunc)
      .collect
    bootstrapTimer.stop
    //TODO: Check whether a reduce is appreciably faster.
    val errorSummaryTimer = LoggingUtils.startCount("Computing summary of error distribution")
    val errorQuantification = errorQuantifier.computeSingleFieldError(bootstrapIterateResults)
    errorSummaryTimer.stop()
    errorQuantification
  }
  
  def doDiagnostic[D, E <: ErrorQuantification](
      data: RDD[D],
      queryFunc: Seq[D] => Double,
      errorQuantifier: ErrorQuantifier[E],
      numDiagnosticSubsamples: Int,
      diagnosticSubsampleSizes: Seq[Int],
      numBootstrapResamples: Int,
      sc: SparkContext,
      numSplits: Int,
      seed: Int)
      (implicit ev: ClassManifest[D]): DiagnosticOutput = {
    val loadTimer = LoggingUtils.startCount("Loading the data")
    //FIXME: Not sure what kind of input we should use.
    val random = new Random(seed)
    val repartitioningSeed = random.nextInt
    val permutedData = data
      .mapPartitionsWithIndex((partitionIdx, partition) => {
        val partitionRandom = new Random(seed + partitionIdx)
        partition.map(item => (partitionRandom.nextInt % numSplits, item))
      })
      .partitionBy(Partitioner.defaultPartitioner(data))
      .persist()
    val dataSize = data.count
    loadTimer.stop
    require(numDiagnosticSubsamples % numSplits == 0) //FIXME
    val diagnosticCollectTimer = LoggingUtils.startCount("Computing and collecting subsample results")
    val partitionToSubsampleSizes = {split: Int => (0 until numDiagnosticSubsamples / numSplits)
      .flatMap(subsampleIdx => (0 until diagnosticSubsampleSizes.size).map(diagnosticSubsampleSizes))}
    val intermediateDiagnosticResults: Seq[(Int, (Double, E))] = permutedData
      .map(_._2)
      .mapPartitionsWithIndex((partitionIdx, partition) => {
        val random = new Random(seed + partitionIdx)
        val subsampleSizes = partitionToSubsampleSizes(partitionIdx)
        val subsampleResults: Seq[(Int, (Double, E))] = subsampleSizes.map(subsampleSize => {
          val subsample = partition.take(subsampleSize).toSeq
          val queryOutput = queryFunc(subsample)
          val bootstrapResamples = (0 until numBootstrapResamples)
            .map(idx => ResampleGenerator.generateLocalResample(subsample, random.nextInt, true))
            .map(queryFunc)
          val bootstrapResult = errorQuantifier.computeSingleFieldError(bootstrapResamples)
          (subsampleSize, (queryOutput, bootstrapResult))
        })
        subsampleResults.iterator
      })
      .collect
    diagnosticCollectTimer.stop
    val diagnosticComputeTimer = LoggingUtils.startCount("Computing diagnostic results locally")
    val resultsBySubsampleSize: Map[Int, Seq[(Double, E)]] = intermediateDiagnosticResults.groupBy(_._1).mapValues(_.map(_._2))
    val diagnosticConf = DiagnosticConf.default
    val diagnosticsPerSubsampleSize = resultsBySubsampleSize
      .map({ case (subsampleSize, results) =>
        val queryOutputs = List(List(errorQuantifier.computeSingleFieldError(results.map(_._1))))
        val bootstrapOutputs = List(List(results.map(_._2)))
        DiagnosticRunner.computeSingleDiagnosticResult(subsampleSize, queryOutputs, bootstrapOutputs, diagnosticConf)
      })
      .toSeq
    val diagnostic = DiagnosticRunner.computeFinalDiagnostic(diagnosticsPerSubsampleSize, diagnosticConf)
    diagnosticComputeTimer.stop
    diagnostic
  }
}