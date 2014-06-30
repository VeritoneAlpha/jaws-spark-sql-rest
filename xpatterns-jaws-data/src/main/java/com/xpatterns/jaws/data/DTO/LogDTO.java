package com.xpatterns.jaws.data.DTO;

import com.google.gson.Gson;

public class LogDTO {

	public String log;
	public String jobId;
	public long timestamp;

	public LogDTO(String log, String jobId) {
		this.log = log;
		this.jobId = jobId;
	}
	
	public LogDTO(String log, String jobId, long timestamp) {
		this.log = log;
		this.jobId = jobId;
		this.timestamp = timestamp;
	}

	public LogDTO() {
	}
	
	
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
	public static LogDTO fromJson(String logDtoJson) {
		return new Gson().fromJson(logDtoJson, LogDTO.class);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + ((log == null) ? 0 : log.hashCode());
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogDTO other = (LogDTO) obj;
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (log == null) {
			if (other.log != null)
				return false;
		} else if (!log.equals(other.log))
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "LogDTO [log=" + log + ", jobId=" + jobId + ", timestamp="
				+ timestamp + "]";
	}

}
