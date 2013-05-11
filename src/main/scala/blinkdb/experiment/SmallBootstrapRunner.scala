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
import blinkdb.BootstrapConf
import blinkdb.ErrorAnalysisConf

object SmallBootstrapRunner {
  /** 
   * Run the bootstrap using a single job, broadcasting all data to all
   * machines.
   */
  def doBroadcastBootstrap[D,E <: ErrorQuantification](
      data: Seq[D],
      queryFunc: Seq[D] => Double,
      errorQuantifier: ErrorQuantifier[E],
      bootstrapConf: BootstrapConf,
      sc: SparkContext,
      numSplits: Int,
      seed: Int): E = {
    val broadcastTimer = LoggingUtils.startCount("Broadcasting the data")
    val dataBroadcast = sc.broadcast(data)
    broadcastTimer.stop
    
    val resampleIndicesParallelizeTimer = LoggingUtils.startCount("Parallelizing %d indices".format(bootstrapConf.numBootstrapResamples))
    val resampleIndices = sc.parallelize(0 until bootstrapConf.numBootstrapResamples, numSplits)
    resampleIndicesParallelizeTimer.stop
    val bootstrapTimer = LoggingUtils.startCount("Running %d bootstraps".format(bootstrapConf.numBootstrapResamples))
    val bootstrapIterateResults: Seq[Double] = resampleIndices
      .map(idx => {
        val broadcastReadTimer = LoggingUtils.startCount("Reading the broadcast variable")
        val broadcastValue = dataBroadcast.value
        broadcastReadTimer.stop
        val singleBootstrapTimer = LoggingUtils.startCount("Running a single bootstrap")
        val resampleData = ResampleGenerator.generateLocalResample(dataBroadcast.value, seed + idx, true)
        println(resampleData.size) //TMP
        val queryTimer = LoggingUtils.startCount("Running the query")
        val result = queryFunc(resampleData)
        queryTimer.stop
        singleBootstrapTimer.stop
        result
      })
      .collect
    bootstrapTimer.stop
    
    val errorSummaryTimer = LoggingUtils.startCount("Computing summary of error distribution")
    val errorQuantification = errorQuantifier.computeSingleFieldError(bootstrapIterateResults)
    errorSummaryTimer.stop()
    errorQuantification
  }
  
  /** Run the bootstrap using a single job for all resamples. */
  def doSingleJobBootstrap[D, A, E <: ErrorQuantification](
      data: RDD[D],
      aggregateLocally: Seq[D] => A,
      combineIntermediates: (A, A) => A,
      toResult: A => Double,
      errorQuantifier: ErrorQuantifier[E],
      bootstrapConf: BootstrapConf,
      sc: SparkContext,
      numSplits: Int,
      seed: Int): E = {
    // Ensure data are materialized in cache.
    val dataLoadTimer = LoggingUtils.startCount("Materializing input data in cache")
    data.persist()
    data.foreach({_ => Unit})
    dataLoadTimer.stop
    
    // For each partition, create numBootstrapResamples partial resamples.
    // For each partial resample, apply localAggregate to it, producing a 100-tuple of As on each partition.
    // Reduce the tuples.  (For now this is just local.  Hopefully the aggregates are not too big.)
    val bootstrapTimer = LoggingUtils.startCount("Running %d bootstraps remotely".format(bootstrapConf.numBootstrapResamples))
    val resampleIntermediateResults = data
      .mapPartitionsWithIndex({(partitionIdx, partition) =>
        val random = new Random(seed + partitionIdx)
        val partitionCache = partition.toSeq
        val localAggregates = (0 until bootstrapConf.numBootstrapResamples)
          .map({_ =>
            val resample = ResampleGenerator.generateLocalResample(partitionCache, random.nextInt, false)
            aggregateLocally(resample)
          })
        Iterator(localAggregates)
      })
      .reduce({(aggregatesA, aggregatesB) =>
        aggregatesA.zip(aggregatesB).map({case (aggregateA, aggregateB) => combineIntermediates(aggregateA, aggregateB)})
      })
    bootstrapTimer.stop
    
    // Map locally to a result.  Hopefully this does not take too long.
    val finalizeLocalResultsTimer = LoggingUtils.startCount("Computing final results from each aggregate")
    val resampleResults = resampleIntermediateResults.map(toResult)
    finalizeLocalResultsTimer.stop
    
    // Compute error locally.
    val errorQuantificationTimer = LoggingUtils.startCount("Computing summary of error distribution")
    val errorQuantification = errorQuantifier.computeSingleFieldError(resampleResults)
    errorQuantificationTimer.stop
    errorQuantification
  }
  
  /** Run the bootstrap using 1 Spark job for each resample. */
  def doMultipleJobBootstrap[D: ClassManifest, A: ClassManifest, E <: ErrorQuantification](
      data: RDD[D],
      aggregator: RDD[D] => Double,
      errorQuantifier: ErrorQuantifier[E],
      bootstrapConf: BootstrapConf,
      sc: SparkContext,
      numSplits: Int,
      seed: Int): E = {
    // Ensure data are materialized in cache.
    val dataLoadTimer = LoggingUtils.startCount("Materializing input data in cache")
    data.persist()
    data.foreach({_ => Unit})
    dataLoadTimer.stop
    
    val bootstrapTimer = LoggingUtils.startCount("Running %d bootstraps remotely".format(bootstrapConf.numBootstrapResamples))
    val resamples = ResampleGenerator.generateResamples(data, bootstrapConf.numBootstrapResamples, seed)
    val resampleResults = resamples.par.map(aggregator)
    bootstrapTimer.stop
    
    // Compute error locally.
    val errorQuantificationTimer = LoggingUtils.startCount("Computing summary of error distribution")
    val errorQuantification = errorQuantifier.computeSingleFieldError(resampleResults.seq)
    errorQuantificationTimer.stop
    errorQuantification
  }
}