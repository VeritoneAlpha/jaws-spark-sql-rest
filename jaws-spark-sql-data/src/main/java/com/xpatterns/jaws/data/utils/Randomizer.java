package com.xpatterns.jaws.data.utils;

import java.util.ArrayList;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

import com.xpatterns.jaws.data.DTO.LogDTO;
import com.xpatterns.jaws.data.DTO.ResultDTO;
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO;

public class Randomizer {

	public static String getRandomString(int nr) {
		return RandomStringUtils.randomAlphabetic(nr);
	}

	public static long getRandomLong() {

		return RandomUtils.nextLong();
	}

	public static ResultDTO getResultDTO() {
		ResultDTO result = new ResultDTO();
		ArrayList<String> schema = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			schema.add(RandomStringUtils.randomAlphabetic(10));
		}

		ArrayList<ArrayList<String>> results = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < 10; i++) {
			ArrayList<String> row = new ArrayList<String>();
			for (int j = 0; j < 10; j++) {
				row.add(RandomStringUtils.randomAlphabetic(10));
			}
			results.add(row);
		}

		result.schema.addAll(schema);
		result.results.addAll(results);

		return result;
	}

	public static LogDTO getLogDTO() {
		return new LogDTO(Randomizer.getRandomString(5000), Randomizer.getRandomString(10));
	}

	public static ScriptMetaDTO createScriptMetaDTO() {
		return new ScriptMetaDTO(RandomUtils.nextLong(), RandomUtils.nextLong(), RandomUtils.nextBoolean(), RandomUtils.nextBoolean());
	}
}
