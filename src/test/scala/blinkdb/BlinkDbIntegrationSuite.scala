package blinkdb

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSuite
import shark.SharkContext
import shark.TestUtils
import shark.SharkEnv

class BlinkDbIntegrationSuite extends FunSuite with ShouldMatchers {
  import SharkSetupUtils._
  import BlinkDbAssertionUtils._
  
  test("Basic aggregate query") {
    val TABLE_NAME = "kv_large"
    val sc = startContext()
    loadKvTable(TABLE_NAME, sc)
    val result = sc.sql("SELECT AVG(val) FROM %s".format(TABLE_NAME))
    println(result)
    assertErrorIs(
      ErrorAnalysis(
        Some(Seq(Seq(StandardDeviationError(.5)))),
        None),
      result)
  }
}

object SharkSetupUtils {
  def startContext(): SharkContext = {
    val WAREHOUSE_PATH = TestUtils.getWarehousePath()
    val METASTORE_PATH = TestUtils.getMetastorePath()
    val MASTER = "local"
    
    val sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)
    sc.sql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" +
        METASTORE_PATH + ";create=true")
    sc.sql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)
    sc.sql("set shark.test.data.path=" + TestUtils.dataFilePath)
    sc
  }
  
  /** @return the name of the table. */
  def loadKvTable(tableName: String, sc: SharkContext) {
    sc.sql("DROP TABLE IF EXISTS %s".format(tableName))
    sc.sql("CREATE TABLE %s (key STRING, val INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'".format(tableName))
    sc.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/%s.txt' INTO TABLE %s".format(tableName, tableName))
  }
}

object BlinkDbAssertionUtils {
  def assertErrorIs[E <: ErrorQuantification](
      expectedErrorAnalysis: ErrorAnalysis[E],
      queryOutput: Seq[String]) {
    assert(queryOutput(0) == expectedErrorAnalysis.toString)
  }
}