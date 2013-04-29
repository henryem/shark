package shark.parse
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory
import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.conf.HiveConf
import shark.SharkConfVars
import shark.BootstrapStage
import spark.RDD

object BlinkDbSemanticAnalyzerFactory {

  /**
   * Return a semantic analyzer for the given ASTNode.
   */
  def get(conf: HiveConf, tree:ASTNode, bootstrapStage: BootstrapStage, inputRdd: Option[RDD[Any]]): BaseSemanticAnalyzer = {
    //TODO: Taking @inputRdd as an argument is a bit inelegant.
    val baseSem = SharkSemanticAnalyzerFactory.get(conf, tree)
    if (baseSem.isInstanceOf[SharkSemanticAnalyzer]) {
      bootstrapStage match {
        case BootstrapStage.InputExtraction => new InputExtractionSemanticAnalyzer(conf)
        case BootstrapStage.BootstrapExecution => new BootstrapSemanticAnalyzer(conf, inputRdd.get)
        case BootstrapStage.DiagnosticExecution => new BootstrapSemanticAnalyzer(conf, inputRdd.get)
      }
    } else {
      baseSem
    }
  }
  
}