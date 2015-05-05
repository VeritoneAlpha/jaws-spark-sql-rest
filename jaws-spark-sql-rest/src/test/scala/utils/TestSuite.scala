package utils

import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.junit.JUnitRunner
import api.DeleteQueryTest
import api.GetQueryInfoTest
import implementation.AvroConverterTest
import implementation.HiveUtilsTest


@RunWith(classOf[JUnitRunner])
class TestSuite extends Suites(new DeleteQueryTest, new GetQueryInfoTest, new AvroConverterTest, new HiveUtilsTest) {
}