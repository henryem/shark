package shark.parse
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory
import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.conf.HiveConf
import shark.SharkConfVars
import shark.BootstrapStage

object BlinkDbSemanticAnalyzerFactory {

  /**
   * Return a semantic analyzer for the given ASTNode.
   */
  def get(conf: HiveConf, tree:ASTNode, bootstrapStage: BootstrapStage): BaseSemanticAnalyzer = {
    val baseSem = SemanticAnalyzerFactory.get(conf, tree)

    if (baseSem.isInstanceOf[SemanticAnalyzer]) {
      baseSem match {
        case BootstrapStage.InputExtraction => new InputExtractionSemanticAnalyzer(conf)
        case BootstrapStage.BootstrapExecution => new BootstrapSemanticAnalyzer(conf)
        case BootstrapStage.DiagnosticExecution => new BootstrapSemanticAnalyzer(conf)
      }
      new SharkSemanticAnalyzer(conf)
    } else if (baseSem.isInstanceOf[ExplainSemanticAnalyzer] &&
        SharkConfVars.getVar(conf, SharkConfVars.EXPLAIN_MODE) == "shark") {
      new SharkExplainSemanticAnalyzer(conf)
    } else {
      baseSem
    }
  }
}