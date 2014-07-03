import org.scalatest.junit.JUnitSuite
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.scalatest.junit.AssertionsForJUnit
import org.apache.spark.scheduler.SharkUtils

/**
 * Created by emaorhian
 */
class SharkUtilsTest extends AssertionsForJUnit {

  @Test def parseHql_test1() { 
    val cmd1 = "select * from test_table"
    val cmd2 = "select count(*) from test_table"
    val hqlScript = cmd1 + "; " + cmd2
    val resultHql = SharkUtils.parseHql(hqlScript)
    assertEquals(2, resultHql.length)
    assertTrue(resultHql.contains(cmd1))
    assertTrue(resultHql.contains(cmd2))
    assertEquals(cmd1, resultHql(0))
    assertEquals(cmd2, resultHql(1))
    
  }
  
  @Test def parseHql_test2() { 
    val hqlScript = "select * from test_table"
    val resultHql = SharkUtils.parseHql(hqlScript)
    assertEquals(1, resultHql.length)
    assertTrue(resultHql.contains(hqlScript))
    
  }
}
