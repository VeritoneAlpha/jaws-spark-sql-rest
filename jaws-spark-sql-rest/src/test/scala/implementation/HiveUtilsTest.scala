package implementation

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.apache.spark.scheduler.HiveUtils
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class HiveUtilsTest extends FunSuite {

  test("split path: ok hdfs") {
    val (namenode, path) = HiveUtils.splitPath("hdfs://devbox:8020/user/ubuntu/testParquet.parquet")
    assert(namenode === "hdfs://devbox:8020")
    assert(path === "/user/ubuntu/testParquet.parquet")

  }

  test("split path: ok tachyon") {
    val (namenode, path) = HiveUtils.splitPath("tachyon://devbox:19998/user/ubuntu/testParquet.parquet")
    assert(namenode === "tachyon://devbox:19998")
    assert(path === "/user/ubuntu/testParquet.parquet")

  }

  test("split path: empty") {
    val trySplit = Try(HiveUtils.splitPath(""))

    assert(trySplit.isFailure === true)
    assert("Invalid file path format : " === trySplit.failed.get.getMessage())
  }
}