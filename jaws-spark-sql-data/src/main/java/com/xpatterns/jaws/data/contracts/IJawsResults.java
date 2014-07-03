package com.xpatterns.jaws.data.contracts;

import java.io.IOException;

import com.xpatterns.jaws.data.DTO.ResultDTO;

public interface IJawsResults {
	public ResultDTO getResults(String uuid) throws IOException;
	public void setResults(String uuid, ResultDTO resultDTO) throws IOException, Exception;
}
