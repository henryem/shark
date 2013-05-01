package shark

sealed trait BootstrapStage
object BootstrapStage {
  case object InputExtraction extends BootstrapStage
  case object BootstrapExecution extends BootstrapStage
  case object DiagnosticExecution extends BootstrapStage
}