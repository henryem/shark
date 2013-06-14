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
import org.apache.hadoop.hive.ql.parse.HiveParser

object BlinkDbSemanticAnalyzerFactory {

  /**
   * Return a semantic analyzer for the given ASTNode.
   */
  def get(conf: HiveConf, tree:ASTNode, analysisStage: ErrorAnalysisStage): BaseSemanticAnalyzer = {
    //TODO: Taking @inputRdd as an argument is a bit inelegant.
    val baseSem = SharkSemanticAnalyzerFactory.get(conf, tree)
    if (baseSem.isInstanceOf[SharkSemanticAnalyzer] && shouldHandleQuery(tree)) {
      analysisStage match {
        case ErrorAnalysisStage.InputExtraction => new InputExtractionSemanticAnalyzer(conf)
        case ErrorAnalysisStage.AnalysisExecution => new BootstrapSemanticAnalyzer(conf) //FIXME: Rename to AnalysisSemanticAnalyzer
      }
    } else {
      baseSem
    }
  }
  
  private def shouldHandleQuery(queryTree: ASTNode): Boolean = {
    if (queryTree.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      //HACK: We don't want to handle create table or CTAS queries.
      false
    } else {
      true
    }
  }
}