package com.xpatterns.jaws.data.DTO;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.xpatterns.jaws.data.DTO.ResultDTO;

public class ResultDTOTest {

	@Test
	public void testTrimmed() throws Exception {
		ResultDTO dto = new ResultDTO();
		
		dto.schema.add(RandomStringUtils.random(3));
		dto.schema.add(RandomStringUtils.random(3));
		
		List<String> row1 = new LinkedList<String>();
		row1.add("aaaa   ");
		row1.add("   bbbb");
		
		List<String> row2 = new LinkedList<String>();
		row2.add("   ccc   ");
		row2.add("ddd");
		
		dto.results.add(row1);
		dto.results.add(row2);
		
		ResultDTO expected = new ResultDTO();
		
		expected.schema.addAll(dto.schema);
		
		List<String> expectedRow1 = new LinkedList<String>();
		expectedRow1.add("aaaa");
		expectedRow1.add("bbbb");
		
		List<String> expectedRow2 = new LinkedList<String>();
		expectedRow2.add("ccc");
		expectedRow2.add("ddd");
		
		expected.results.add(expectedRow1);
		expected.results.add(expectedRow2);
		
		
		Assert.assertEquals(expected, ResultDTO.trimResults(dto));
	}
	
	
}
