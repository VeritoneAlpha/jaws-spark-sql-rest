package com.xpatterns.jaws.data.DTO;

import com.google.gson.Gson;

public class ScriptMetaDTO {

	public Long nrOfResults;
	public Long maxNrOfResults;
	public boolean resultsInCassandra;
	public boolean isLimited;

	public ScriptMetaDTO(Long nbOfResults, Long maxNrOfResults, Boolean resultsInCassandra, Boolean isLimited) {
		this.nrOfResults = nbOfResults;
		this.maxNrOfResults = maxNrOfResults;
		this.resultsInCassandra = resultsInCassandra;
		this.isLimited = isLimited;
	}

	public ScriptMetaDTO() {
	}
	
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
	public static ScriptMetaDTO fromJson(String scriptDTOjson) {
		return new Gson().fromJson(scriptDTOjson, ScriptMetaDTO.class);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((maxNrOfResults == null) ? 0 : maxNrOfResults.hashCode());
		result = prime * result + ((nrOfResults == null) ? 0 : nrOfResults.hashCode());
		result = prime * result + (resultsInCassandra ? 1231 : 1237);
		result = prime * result + (isLimited ? 1231 : 1237);
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
		ScriptMetaDTO other = (ScriptMetaDTO) obj;
		if (maxNrOfResults == null) {
			if (other.maxNrOfResults != null)
				return false;
		} else if (!maxNrOfResults.equals(other.maxNrOfResults))
			return false;
		if (nrOfResults == null) {
			if (other.nrOfResults != null)
				return false;
		} else if (!nrOfResults.equals(other.nrOfResults))
			return false;
		if (resultsInCassandra != other.resultsInCassandra)
	            return false;
		if (isLimited != other.isLimited)
            return false;
		return true;
	}

	@Override
	public String toString() {
		return "ScriptMetaDTO [nrOfResults=" + nrOfResults + ", maxNrOfResults=" + maxNrOfResults + ", resultsInCassandra=" + resultsInCassandra + ", isLimited=" + isLimited + "]";
	}

}
