package blinkdb
import shark.LogHelper

/** The output of a single run of a query. */
case class SingleQueryIterateOutput(rows: Seq[Seq[Double]], numRows: Int, numFields: Int) {
  require(rows.size == numRows)
  require(rows.size == 0 || rows(0).size == numFields)
}

/**
 * A container for the results of error analysis on an approximate query.
 * Includes:
 *   * Quantifications of estimation error for each output column or row.
 *   * The output of a diagnostic for the error estimation process itself.
 */
case class ErrorAnalysis[E <: ErrorQuantification](
    errorQuantifications: Option[Seq[Seq[E]]],
    diagnosticOutput: Option[DiagnosticOutput])

/** The output of a diagnostic for an error estimation process. */
case class DiagnosticOutput(isPassed: Boolean)

/**
 * A quantification of error for a single estimate, typically estimated by one
 * or more bootstrap runs or by computing a closed-form estimate.
 */
trait ErrorQuantification {
  def toDouble: Double
}

case class StandardDeviationError(stdDev: Double) extends ErrorQuantification {
  override def toDouble = stdDev
}

object ErrorQuantifications {
  implicit def toDouble(e: ErrorQuantification) = e.toDouble
}

trait ErrorQuantifier[Q <: ErrorQuantification] {
  def computeError(bootstrapOutputs: Seq[SingleQueryIterateOutput]): Seq[Seq[Q]] = {
    //TODO: If there are no bootstrap outputs, we don't know how many fields
    // there would have been.  Currently we just return no error
    // quantifications.
    //TODO: If the outputs don't match expectations, warn and throw them away.
    // If more than a couple are thrown away, fail.
    if (bootstrapOutputs.size == 0) {
      return Nil
    }
    val (numFields, numRows) = bootstrapOutputs.map(output => (output.numFields, output.numRows)).head
    for (output <- bootstrapOutputs) {
      require(output.numFields == numFields, "The number of columns selected in the bootstrap must equal the number of columns in the original query's output.")
      require(output.numRows == numRows, "The number of rows in bootstrap output must equal the number of rows in the original query's output.")
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
  
  def standardDeviation(numbers: Seq[Double]): Double = {
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