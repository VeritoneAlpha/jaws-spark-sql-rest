package api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Suites

@RunWith(classOf[JUnitRunner])
class TestSuite extends Suites(new JawsIsUpTest, new RunApiTest, new RunHiveApiTest, new ParquetManagementApiTest) {
}