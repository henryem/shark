package blinkdb
import shark.LogHelper

/** The output of a single run of a bootstrap. */
case class BootstrapOutput(rows: Seq[Seq[Double]], numRows: Int, numFields: Int) {
  require(rows.size == numRows)
  require(rows.size == 0 || rows(0).size == numFields)
}

/**
 * A quantification of error, typically estimated by one or more bootstrap
 * runs or by computing a closed-form estimate.
 */
trait ErrorQuantification

case class StandardDeviationError(stdDev: Double) extends ErrorQuantification

trait ErrorQuantifier[Q <: ErrorQuantification] {
  def computeError(bootstrapOutputs: Seq[BootstrapOutput]): Seq[Seq[Q]] = {
    //TODO: If there are no bootstrap outputs, we don't know how many fields
    // there would have been.  Currently we just return no error
    // quantifications.
    if (bootstrapOutputs.size == 0) {
      return Nil
    }
    val (numFields, numRows) = bootstrapOutputs.map(output => (output.numFields, output.numRows)).head
    for (output <- bootstrapOutputs) {
      require(output.numFields == numFields)
      require(output.numRows == numRows)
    }
    (0 until numRows).map(rowIdx => {
      val currentRowFromEachBootstrap = bootstrapOutputs.map(_.rows(rowIdx))
      (0 until numFields).map(fieldIdx => {
        val currentFieldFromEachCurrentRow = currentRowFromEachBootstrap.map(_(fieldIdx))
        computeSingleFieldError(currentFieldFromEachCurrentRow)
      })
    })
  }
  
  def computeSingleFieldError(singleFieldValues: Seq[Double]): Q
}

case object StandardDeviationErrorQuantifier extends ErrorQuantifier[StandardDeviationError] with LogHelper {
  override def computeSingleFieldError(singleFieldValues: Seq[Double]): StandardDeviationError = {
    StandardDeviationError(standardDeviation(singleFieldValues))
  }
  
  private def standardDeviation(numbers: Seq[Double]): Double = {
    logDebug("Computing standard deviation of %s".format(numbers))
    //TODO: This is inefficient.
    val count = numbers.size
    val sum = numbers.sum
    val sumSq = numbers.map(number => number*number).sum
    if (count == 0) {
      0.0
    } else {
      math.sqrt((sumSq - sum*sum/count) / count)
    }
  }
}