package blinkdb.parse
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory
import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.conf.HiveConf
import shark.SharkConfVars
import spark.RDD
import shark.parse.SharkSemanticAnalyzerFactory
import shark.parse.SharkSemanticAnalyzer
import blinkdb.ErrorAnalysisStage

object BlinkDbSemanticAnalyzerFactory {

  /**
   * Return a semantic analyzer for the given ASTNode.
   */
  def get(conf: HiveConf, tree:ASTNode, analysisStage: ErrorAnalysisStage, inputRdd: Option[RDD[Any]]): BaseSemanticAnalyzer = {
    //TODO: Taking @inputRdd as an argument is a bit inelegant.
    val baseSem = SharkSemanticAnalyzerFactory.get(conf, tree)
    if (baseSem.isInstanceOf[SharkSemanticAnalyzer]) {
      analysisStage match {
        case ErrorAnalysisStage.InputExtraction => new InputExtractionSemanticAnalyzer(conf)
        case ErrorAnalysisStage.BootstrapExecution => new BootstrapSemanticAnalyzer(conf, inputRdd.get)
        case ErrorAnalysisStage.DiagnosticExecution => new BootstrapSemanticAnalyzer(conf, inputRdd.get)
      }
    } else {
      baseSem
    }
  }
  
}