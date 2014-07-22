//
//import org.apache.spark.scheduler.SharkUtils
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.BeforeAndAfter
//import shark.SharkContext
//import shark.SharkEnv
//import utils.TestUtils
//import com.xpatterns.jaws.data.contracts.IJawsLogging
//import org.scalatest.FunSuite
//import org.scalamock.proxy.ProxyMockFactory
//import org.scalatest.matchers.ShouldMatchers
//import com.xpatterns.jaws.data.utils.Utils
//import actors.Configuration
//
///**
// * Created by emaorhian
// */
//
//class SharkUtilsTest extends FunSuite with MockFactory with BeforeAndAfter with ProxyMockFactory {
//
//  //class SharkUtilsTest extends FunSuite with ShouldMatchers with BeforeAndAfter with org.scalamock.scalatest.proxy.MockFactory {
//  var sc: SharkContext = _
//
//  before {
//    if (sc == null) {
//      sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", "local")
//
//      sc.runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + TestUtils.getMetastorePath("SharkUtilsTest") + ";create=true")
//      sc.runSql("set hive.metastore.warehouse.dir=" + TestUtils.getWarehousePath("SharkUtilsTest"))
//      sc.runSql("set shark.test.data.path=" + TestUtils.dataFilePath)
//
//      loadTables()
//    }
//
//    sc
//  }
//
//  def loadTables() = synchronized {
//    require(sc != null, "call init() to instantiate a SharkContext first")
//
//    // test
//    sc.runSql("drop table if exists test")
//    sc.runSql("CREATE TABLE test (key INT, val STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"")
//    sc.runSql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test")
//  }
//
//  def getHadoopConf(): org.apache.hadoop.conf.Configuration = {
//    val configuration = new org.apache.hadoop.conf.Configuration()
//    configuration.setBoolean(Utils.FORCED_MODE, true)
//
//    configuration.set("fs.defaultFS", "file://testHdfs/")
//    configuration
//  }
//
//  // **************** TESTS *********************
//
//  test("parseHql_test1") {
//    val cmd1 = "select * from test_table"
//    val cmd2 = "select count(*) from test_table"
//    val hqlScript = cmd1 + "; " + cmd2
//    val resultHql = SharkUtils.parseHql(hqlScript)
//    assert(resultHql.length === 2)
//    assert(resultHql.contains(cmd1))
//    assert(resultHql.contains(cmd2))
//    assert(cmd1 === resultHql(0))
//    assert(cmd2 === resultHql(1))
//
//  }
//
//  test("parseHql_test2") {
//    val hqlScript = "select * from test_table"
//    val resultHql = SharkUtils.parseHql(hqlScript)
//    assert(1 === resultHql.length)
//    assert(resultHql.contains(hqlScript))
//
//  }
//
//  test("runCmd_test1") {
//    val loging = mock[IJawsLogging]
//    loging.expects('setMetaInfo)(*, *)
//
//    val result = SharkUtils.runCmd("show databases", sc, "uuid", loging)
//
//    assert(result.getResults.length === 1)
//    assert(result.getResults()(0)(0) === "default")
//
//  }
//
//  test("runCmdRdd_test_limited and few results") {
//    val loging = mock[IJawsLogging]
//    loging.expects('setMetaInfo)(*, *)
//
//    val result = SharkUtils.runCmdRdd("select * from test", sc, 100, "uuid", true, 2, false, "localhost", loging, getHadoopConf)
//    assert(result.getResults.length === 2)
//    assert(result.getResults()(0).length === 2)
//
//  }
//  
// 
//   test("runCmdRdd_test_limited, lots of results and last command") {
//    val loging = mock[IJawsLogging]
//    loging.expects('setMetaInfo)(*, *)
//
//    val result = SharkUtils.runCmdRdd("select * from test", sc, 100, "uuid", true, 1000, true, "localhost", loging, getHadoopConf)
//    assert(result === null)
//
//
//  }
//   
//    test("runCmdRdd_test_limited, lots of results and not last command") {
//    val loging = mock[IJawsLogging]
//    loging.expects('setMetaInfo)(*, *)
//
//    val result = SharkUtils.runCmdRdd("select * from test", sc, 3, "uuid", true, 1000, false, "localhost", loging, getHadoopConf)
//    assert(result.getResults.length === 3)
//    assert(result.getResults()(0).length === 2)
//
//  }
//  
//   
//   test("runCmdRdd_test_not limited and last command") {
//    val loging = mock[IJawsLogging]
//    loging.expects('setMetaInfo)(*, *)
//
//    val result = SharkUtils.runCmdRdd("select * from test", sc, 100, "uuid", false, 1000, true, "localhost", loging, getHadoopConf)
//    assert(result === null)
//
//
//  }
//   
//    test("runCmdRdd_test_not limited and not last command") {
//    val loging = mock[IJawsLogging]
//    loging.expects('setMetaInfo)(*, *)
//
//    val result = SharkUtils.runCmdRdd("select * from test", sc, 2, "uuid", false, 1000, false, "localhost", loging, getHadoopConf)
//    assert(result.getResults.length === 2)
//    assert(result.getResults()(0).length === 2)
//
//  }
//    
//    test("runCmdRdd_test_set") {
//    val loging = mock[IJawsLogging]
//   
//    val result = SharkUtils.runCmdRdd("set test.val=1", sc, 2, "uuid", false, 1000, false, "localhost", loging, getHadoopConf)
//   
//  }
//  
//   
//}
