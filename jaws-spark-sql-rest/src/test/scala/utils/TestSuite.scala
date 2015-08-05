package utils

import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.junit.JUnitRunner
import api.{QueryNameTest, DeleteQueryTest, GetQueryInfoTest}
import implementation.HiveUtilsTest


@RunWith(classOf[JUnitRunner])
class TestSuite extends Suites(new DeleteQueryTest, new QueryNameTest, new GetQueryInfoTest, new HiveUtilsTest) {
}