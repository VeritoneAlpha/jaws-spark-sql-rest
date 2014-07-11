
import org.apache.spark.scheduler.SharkUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import shark.SharkContext
import shark.SharkEnv
import utils.TestUtils
import com.xpatterns.jaws.data.contracts.IJawsLogging
import org.scalatest.FunSuite

import org.scalamock.proxy.ProxyMockFactory

/**
 * Created by emaorhian
 */

class SharkUtilsTest extends FunSuite with MockFactory with BeforeAndAfter with ProxyMockFactory {

  var sc: SharkContext = _



   before {
    if (sc == null) {
      sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", "local")

      sc.runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + TestUtils.getMetastorePath("SharkUtilsTest") + ";create=true")
      sc.runSql("set hive.metastore.warehouse.dir=" + TestUtils.getWarehousePath("SharkUtilsTest"))
      sc.runSql("set shark.test.data.path=" + TestUtils.dataFilePath)

      loadTables()
    }
    
    sc
  }

  
   def loadTables() = synchronized {
    require(sc != null, "call init() to instantiate a SharkContext first")

    // test
    sc.runSql("drop table if exists test")
    sc.runSql("CREATE TABLE test (key INT, val STRING)")
    sc.runSql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/kv1.txt' INTO TABLE test")
   }
  
  test("parseHql_test1") {
    val cmd1 = "select * from test_table"
    val cmd2 = "select count(*) from test_table"
    val hqlScript = cmd1 + "; " + cmd2
    val resultHql = SharkUtils.parseHql(hqlScript)
    assert(resultHql.length === 2)
    assert(resultHql.contains(cmd1))
    assert(resultHql.contains(cmd2))
    assert(cmd1 === resultHql(0))
    assert(cmd2 === resultHql(1))

  }

  test("parseHql_test2") {
    val hqlScript = "select * from test_table"
    val resultHql = SharkUtils.parseHql(hqlScript)
    assert(1 === resultHql.length)
    assert(resultHql.contains(hqlScript))

  }

    test("runCmd_test1") {
       val loging= mock[IJawsLogging]
      loging expects 'setMetaInfo
       val result = SharkUtils.rrunCmd("select * from test", sc, "uuid", loging)

      println(result)
    }
}
