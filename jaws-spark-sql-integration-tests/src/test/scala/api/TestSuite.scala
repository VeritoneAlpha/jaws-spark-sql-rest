package api

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Args
import org.scalatest.Suite
import org.scalatest.Suites

@RunWith(classOf[JUnitRunner])
class TestSuite extends Suites(new JawsIsUpTest, new RunApiTest, new RunHiveApiTest, new ParquetManagementApiTest) {
}