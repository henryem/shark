package blinkdb
import org.apache.commons.configuration.SystemConfiguration
import org.apache.commons.configuration.AbstractConfiguration
import java.util.Properties

//TODO: Populate this from HiveConf instead of Java system properties.
case class BlinkDbConf(
    errorAnalysisConf: ErrorAnalysisConf)

object BlinkDbConf {
  val LIST_DELIMITER = ","
  
  def fromJavaOpts(): BlinkDbConf = fromConf(new SystemConfiguration())
  
  def fromConf(conf: AbstractConfiguration) = {
    BlinkDbConf(ErrorAnalysisConf.fromConf(conf))
  }
  
  /**
   * Get a list from the property @propertyName in @conf.  @conf should return
   * a string of LIST_DELIMITER-separated @A.  Each element of the list is
   * parsed using @parser.  If any element can't be parsed, or if the property
   * doesn't exist in @conf, @defaultList is returned.
   */
  def getList[A: ClassManifest](conf: AbstractConfiguration, propertyName: String, parser: String => Option[A], defaultList: Seq[A]): Seq[A] = {
    Option(conf.getString(propertyName, null))
      .map(_.split(LIST_DELIMITER))
      .map(_.map(parser))
      .filter(_.forall(_.isDefined))
      .map(_.flatten.asInstanceOf[Seq[A]])
      .getOrElse(defaultList)
  }
  
  def toInt(intStr: String): Option[Int] = {
    try {
      Some(intStr.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}

case class ErrorAnalysisConf(
    bootstrapConf: BootstrapConf,
    diagnosticConf: DiagnosticConf)

object ErrorAnalysisConf {
  def fromConf(conf: AbstractConfiguration): ErrorAnalysisConf = {
    ErrorAnalysisConf(BootstrapConf.fromConf(conf), DiagnosticConf.fromConf(conf))
  }
}
    
case class BootstrapConf(
    doBootstrap: Boolean,
    numBootstrapResamples: Int)

object BootstrapConf {
  val DEFAULT_DO_BOOTSTRAP = true
  val DEFAULT_NUM_BOOTSTRAP_RESAMPLES = 100
  
  def fromConf(conf: AbstractConfiguration): BootstrapConf = {
    BootstrapConf(
        conf.getBoolean("blinkdb.bootstrap.doBootstrap", DEFAULT_DO_BOOTSTRAP),
        conf.getInt("blinkdb.bootstrap.numBootstrapResamples", DEFAULT_NUM_BOOTSTRAP_RESAMPLES))
  }
}

//TODO: Move parameter documentation here.
case class DiagnosticConf(
    doDiagnostic: Boolean,
    numDiagnosticSubsamples: Int,
    diagnosticSubsampleSizes: Seq[Int],
    acceptableRelativeMeanDeviation: Double,
    acceptableRelativeStandardDeviation: Double,
    acceptableProportionNearGroundTruth: Double,
    acceptableGroundTruthDeviation: Double)

object DiagnosticConf {
  val DEFAULT_DO_DIAGNOSTIC = true
  val DEFAULT_NUM_DIAGNOSTIC_SUBSAMPLES = 100
  val DEFAULT_DIAGNOSTIC_SUBSAMPLE_SIZES = List(100, 500, 1000)
  
  // An acceptable level of deviation between the average bootstrap output
  // and ground truth, normalized by ground truth.  This is c_1 in the
  // diagnostics paper.
  val DEFAULT_ACCEPTABLE_RELATIVE_MEAN_DEVIATION = .2
  
  // An acceptable level of standard deviation among bootstrap outputs,
  // normalized by ground truth.  This is c_2 in the diagnostics paper.
  val DEFAULT_ACCEPTABLE_RELATIVE_STANDARD_DEVIATION = .2
  
  // An acceptable proportion of bootstrap outputs that are within
  // ACCEPTABLE_DEVIATION of ground truth.  This is \alpha in the diagnostics
  // paper.
  val DEFAULT_ACCEPTABLE_PROPORTION_NEAR_GROUND_TRUTH = .95
  // See ACCEPTABLE_PROPORTION_NEAR_GROUND_TRUTH above.  This is c_3 in the
  // diagnostics paper.
  val DEFAULT_ACCEPTABLE_GROUND_TRUTH_DEVIATION = .2
  
  def fromConf(conf: AbstractConfiguration): DiagnosticConf = {
    DiagnosticConf(
        conf.getBoolean("blinkdb.diagnostic.doDiagnostic", DEFAULT_DO_DIAGNOSTIC),
        conf.getInt("blinkdb.diagnostic.numDiagnosticSubsamples", DEFAULT_NUM_DIAGNOSTIC_SUBSAMPLES),
        BlinkDbConf.getList(conf, "blinkdb.diagnostic.diagnosticSubsampleSizes", BlinkDbConf.toInt, DEFAULT_DIAGNOSTIC_SUBSAMPLE_SIZES),
        conf.getDouble("blinkdb.diagnostic.acceptableRelativeMeanDeviation", DEFAULT_ACCEPTABLE_RELATIVE_MEAN_DEVIATION),
        conf.getDouble("blinkdb.diagnostic.acceptableRelativeStandardDeviation", DEFAULT_ACCEPTABLE_RELATIVE_STANDARD_DEVIATION),
        conf.getDouble("blinkdb.diagnostic.acceptableProportionNearGroundTruth", DEFAULT_ACCEPTABLE_PROPORTION_NEAR_GROUND_TRUTH),
        conf.getDouble("blinkdb.diagnostic.acceptableGroundTruthDeviation", DEFAULT_ACCEPTABLE_GROUND_TRUTH_DEVIATION))
  }
}
