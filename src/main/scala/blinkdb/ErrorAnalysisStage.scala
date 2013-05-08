package blinkdb

sealed trait ErrorAnalysisStage
object ErrorAnalysisStage {
  case object InputExtraction extends ErrorAnalysisStage
  case object BootstrapExecution extends ErrorAnalysisStage
  case object DiagnosticExecution extends ErrorAnalysisStage
}