package blinkdb

sealed trait ErrorAnalysisStage
object ErrorAnalysisStage {
  case object InputExtraction extends ErrorAnalysisStage
  case object AnalysisExecution extends ErrorAnalysisStage
}