package com.xpatterns.jaws.data.contracts;

import java.io.IOException;
import java.util.Collection;

import com.xpatterns.jaws.data.DTO.LogDTO;
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO;
import com.xpatterns.jaws.data.DTO.StateDTO;
import com.xpatterns.jaws.data.utils.JobType;


public interface IJawsLogging {
	
	public void setState(String uuid, JobType type) throws Exception;
	public void setScriptDetails(String uuid, String scriptDetails) throws Exception;
	public void addLog(String uuid, String jobId, Long time, String log) throws IOException, Exception;
	public void setMetaInfo(String uuid, ScriptMetaDTO metainfo) throws Exception;
	
	public JobType getState(String uuid) throws IOException;
	public String getScriptDetails(String uuid) throws IOException;
	public Collection<LogDTO> getLogs(String uuid, Long time, int limit) throws IOException;
	public ScriptMetaDTO getMetaInfo(String uuid) throws IOException;
	
	Collection<StateDTO> getStateOfJobs(String uuid, int limit) throws IOException;
}