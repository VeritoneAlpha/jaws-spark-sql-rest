package com.xpatterns.jaws.data.impl;


import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.xpatterns.jaws.data.DTO.ResultDTO;
import com.xpatterns.jaws.data.contracts.IJawsResults;
import com.xpatterns.jaws.data.utils.Randomizer;

public class JawsResultsTest {

	private static IJawsResults resultsDal;

	@BeforeClass
	public static void setUp() throws Exception {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"test-application-context.xml");
		resultsDal = (IJawsResults) context.getBean("resultsDal");

	}

	@Test
	public void testWriteReadResults() throws IOException, Exception {
		String uuid = Randomizer.getRandomString(10);
		ResultDTO resultDTO = Randomizer.getResultDTO();
		resultsDal.setResults(uuid, resultDTO);
		
		ResultDTO results = resultsDal.getResults(uuid);
		
		Assert.assertEquals(resultDTO, results);
	}

}
