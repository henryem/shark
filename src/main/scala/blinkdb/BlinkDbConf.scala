package blinkdb

//TODO: Populate this to HiveConf and pass it around, to replace hard-coded
// values.
case class BlinkDbConf(
    bootstrapConf: BootstrapConf,
    diagnosticConf: DiagnosticConf)

case class BootstrapConf(
    numBootstrapResamples: Int)

case class DiagnosticConf(
    numDiagnosticSubsamples: Int,
    diagnosticSubsampleSizes: Seq[Int])