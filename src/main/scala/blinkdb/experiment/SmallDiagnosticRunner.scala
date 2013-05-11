package blinkdb.experiment

import blinkdb.ErrorQuantification
import spark.RDD
import blinkdb.ErrorQuantifier
import blinkdb.ErrorAnalysisConf
import spark.SparkContext
import blinkdb.util.LoggingUtils
import blinkdb.DiagnosticOutput
import java.util.Random
import blinkdb.DiagnosticRunner
import blinkdb.DiagnosticConf
import spark.SparkContext._
import spark.Partitioner
import blinkdb.util.ResampleGenerator
import blinkdb.util.RddUtils._

object SmallDiagnosticRunner {
  def doSingleJobDiagnostic[D, E <: ErrorQuantification](
      data: RDD[D],
      queryFunc: Seq[D] => Double,
      errorQuantifier: ErrorQuantifier[E],
      errorAnalysisConf: ErrorAnalysisConf,
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
    require(errorAnalysisConf.diagnosticConf.numDiagnosticSubsamples % numSplits == 0) //FIXME
    val diagnosticCollectTimer = LoggingUtils.startCount("Computing and collecting subsample results")
    val partitionToSubsampleSizes = {split: Int => (0 until errorAnalysisConf.diagnosticConf.numDiagnosticSubsamples / numSplits)
      .flatMap(subsampleIdx => (0 until errorAnalysisConf.diagnosticConf.diagnosticSubsampleSizes.size).map(errorAnalysisConf.diagnosticConf.diagnosticSubsampleSizes))}
    val intermediateDiagnosticResults: Seq[(Int, (Double, E))] = permutedData
      .map(_._2)
      .mapPartitionsWithIndex((partitionIdx, partition) => {
        val random = new Random(seed + partitionIdx)
        val subsampleSizes = partitionToSubsampleSizes(partitionIdx)
        val subsampleResults: Seq[(Int, (Double, E))] = subsampleSizes.map(subsampleSize => {
          val subsample = partition.take(subsampleSize).toSeq
          val queryOutput = queryFunc(subsample)
          val bootstrapResamples = (0 until errorAnalysisConf.bootstrapConf.numBootstrapResamples)
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
  
  /** 
   * A minimal-overhead diagnostic, but using 1 task per resample.  For
   * example, with 100 subsamples of each of 3 sizes, and 100 resamples per
   * subsample, there will be 30,000 tasks.
   * 
   * @queryFunc should expect its input RDD to have only 1 partition.  For
   *   efficiency, it should not repartition the input.
   */
  def doMultipleJobDiagnostic[D, E <: ErrorQuantification](
      data: RDD[D],
      queryFunc: RDD[D] => Double,
      errorQuantifier: ErrorQuantifier[E],
      errorAnalysisConf: ErrorAnalysisConf,
      sc: SparkContext,
      numSplits: Int,
      seed: Int)
      (implicit ev: ClassManifest[D]): DiagnosticOutput = {
    val loadTimer = LoggingUtils.startCount("Loading the data")
    val random = new Random(seed)
    val repartitioningSeed = random.nextInt
    val permutedData = data
      .mapPartitionsWithIndex((partitionIdx, partition) => {
        val partitionRandom = new Random(seed + partitionIdx)
        partition.map(item => (partitionRandom.nextInt % numSplits, item))
      })
      .partitionBy(Partitioner.defaultPartitioner(data))
      .map(_._2)
      .persist()
    val partitionSizes = data
      .mapPartitionsWithIndex({(partitionIdx, partition) => Iterator((partitionIdx, partition.size))})
      .collectAsMap
    val singlePartitionRdds = (0 until permutedData.partitions.size)
      .map(permutedData.getSinglePartition)
    loadTimer.stop
    
    require(errorAnalysisConf.diagnosticConf.numDiagnosticSubsamples % numSplits == 0) //FIXME
    
    val subsampleToPartition: (Int, Int) => Int = {(subsampleSize, subsampleIdx) =>
      subsampleIdx % numSplits
    }
    // For each resample, make a resampled RDD and compute the query on it.
    val diagnosticCollectTimer = LoggingUtils.startCount("Computing and collecting subsample results")
    val intermediateDiagnosticResults = for (
        subsampleSize <- errorAnalysisConf.diagnosticConf.diagnosticSubsampleSizes.par;
        subsampleIdx <- (0 until errorAnalysisConf.diagnosticConf.numDiagnosticSubsamples).par;
        partitionIdx = subsampleToPartition(subsampleSize, subsampleIdx);
        baseRdd = singlePartitionRdds(partitionIdx);
        baseRddSize = partitionSizes(partitionIdx);
        subsampleSeed = random.nextInt;
        subsample <- ResampleGenerator.generateSubsamples(baseRdd, 1, subsampleSize, baseRddSize, subsampleSeed).par)
        yield {
      val subsampleQuery = queryFunc(subsample)
      val resampleSeed = random.nextInt;
      val resampleQueryResults: Seq[Double] = ResampleGenerator.generateResamples(subsample, errorAnalysisConf.bootstrapConf.numBootstrapResamples, resampleSeed)
        .par
        .map(queryFunc)
        .seq
      val error = errorQuantifier.computeSingleFieldError(resampleQueryResults)
      (subsampleSize, (subsampleQuery, error))
    }
    diagnosticCollectTimer.stop
    
    val diagnosticComputeTimer = LoggingUtils.startCount("Computing diagnostic results locally")
    val resultsBySubsampleSize: Map[Int, Seq[(Double, E)]] = intermediateDiagnosticResults.seq.groupBy(_._1).mapValues(_.map(_._2))
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