package com.xpatterns.jaws.data.DTO;

import com.google.gson.Gson;
import com.xpatterns.jaws.data.utils.QueryState;

public class StateDTO {

	public QueryState state;
	public String uuid;
	
	public StateDTO(){
		state = QueryState.NOT_FOUND;
		uuid = null;
	}
	
	public StateDTO(QueryState state, String uuid){
		this.state = state;
		this.uuid = uuid;
	}
	
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
	public static StateDTO fromJson(String scriptDTOjson) {
		return new Gson().fromJson(scriptDTOjson, StateDTO.class);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((state == null) ? 0 : state.hashCode());
		result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
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
		StateDTO other = (StateDTO) obj;
		if (state != other.state)
			return false;
		if (uuid == null) {
			if (other.uuid != null)
				return false;
		} else if (!uuid.equals(other.uuid))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "StateDTO [state=" + state + ", uuid=" + uuid + "]";
	}
	
}
