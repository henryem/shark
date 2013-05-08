package blinkdb

case class BlinkDbConf(
    bootstrapConf: BootstrapConf,
    diagnosticConf: DiagnosticConf)

case class BootstrapConf(
    numBootstrapResamples: Int)

case class DiagnosticConf(
    numDiagnosticSubsamples: Int,
    diagnosticSubsampleSizes: Seq[Int])