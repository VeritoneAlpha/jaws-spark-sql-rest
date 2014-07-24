
package com.xpatterns.jaws.data.DTO

import org.scalatest.FunSuite
import org.apache.commons.lang.RandomStringUtils


/**
 * Created by emaorhian
 */

class ResultTest extends FunSuite  {

  test("test trim") {
		
		var schema = Array[Column]()
		for (i <- 0 to 2) {
			schema = schema ++ Array(new Column(RandomStringUtils.randomAlphabetic(10) , RandomStringUtils.randomAlphabetic(10)))
		 
		}

		var results = Array[Array[String]]()

		var row1 = Array [String]()
		row1 = row1 ++ Array("aaaa   ")
		row1 = row1 ++ Array("   bbbb")
		
		var row2 = Array [String]()
		row2 = row2 ++ Array("   ccc   ");
		row2 = row2 ++ Array("ddd");
		
		results = results ++ Array(row1)
		results = results ++ Array(row2)
		
		
		var value = new Result(schema , results)
		
		var expectedRow1 = Array [String]()
		expectedRow1 = expectedRow1 ++ Array("aaaa");
		expectedRow1 = expectedRow1 ++ Array("bbbb");
		
		var expectedRow2 = Array [String]()
		expectedRow2 = expectedRow2 ++ Array("ccc");
		expectedRow2 = expectedRow2 ++ Array("ddd");
		
		var expectedResults = Array[Array[String]]()
		expectedResults = expectedResults ++ Array(expectedRow1)
		expectedResults = expectedResults ++ Array(expectedRow2)
		
		var expected = new Result (schema, expectedResults)
		var resulted = Result.trimResults(value)

		assert(expected === resulted)
	  }
  
}