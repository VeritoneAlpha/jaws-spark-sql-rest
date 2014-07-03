package com.xpatterns.jaws.data.DTO;

import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;

import scala.Tuple2;
import shark.api.ColumnDesc;
import shark.api.ResultSet;
import shark.api.Row;

public class ResultDTO {

	public List<String> schema;
	public List<List<String>> results;

	public ResultDTO() {
		schema = new LinkedList<String>();
		results = new LinkedList<List<String>>();
	}

	public ResultDTO(List<String> schema, List<List<String>> results) {
		this.schema = schema;
		this.results = results;
	}
	
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
	public static ResultDTO fromJson(String resultDTOjson) {
		return new Gson().fromJson(resultDTOjson, ResultDTO.class);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((results == null) ? 0 : results.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
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
		ResultDTO other = (ResultDTO) obj;
		if (results == null) {
			if (other.results != null)
				return false;
		} else if (!results.equals(other.results))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ResultDTO [schema=" + schema + ", results=" + results + "]";
	}

	public static ResultDTO fromResultSet(ResultSet resultSet) {
		ResultDTO result = new ResultDTO();

		if (resultSet != null) {
			// add schema
			for (ColumnDesc desc : resultSet.getSchema()) {
				result.schema.add(desc.name());
			}
			// add results
			for (Object[] res : resultSet.getResults()) {
				List<String> row = new LinkedList<String>();
				for (int i = 0; i < res.length; i++) {
					Object field = res[i];
					if (field == null) {
						field = "NULL";
					}
					row.add(field.toString());
				}
				result.results.add(row);

			}
		}

		return result;
	}

	public static ResultDTO fromRDD(ColumnDesc[] columns, List<Row> rows) {
		ResultDTO result = new ResultDTO();
		// add schema
		for (ColumnDesc column : columns) {
			result.schema.add(column.name());
		}

		// add results
		for (Row rddRow : rows) {
			List<String> row = new LinkedList<String>();
			for (int fieldIndex = 0; fieldIndex < columns.length; fieldIndex++) {
				Object field = rddRow.get(fieldIndex);
				if (field == null) {
					field = "NULL";
				}
				row.add(field.toString());
			}
			result.results.add(row);
		}

		return result;
	}

	public static ResultDTO trimResults(ResultDTO result) {
		ResultDTO trimmedResult = new ResultDTO();
		if (result != null) {
			trimmedResult.schema.addAll(result.schema);

			for (List<String> row : result.results) {
				List<String> trimmedRow = new LinkedList<String>();
				for (int index = 0; index < row.size(); index++) {
					trimmedRow.add(row.get(index).trim());
				}
				trimmedResult.results.add(trimmedRow);
			}
		}

		return trimmedResult;
	}

	public static ResultDTO fromTuples(List<String> schema_, List<Tuple2<Object, Object[]>> filteredResults) {
		ResultDTO result = new ResultDTO();

		// add schema
		result.schema.addAll(schema_);
		
		for (Tuple2<Object, Object[]> tuple : filteredResults) {
			Object [] row = (Object[]) tuple._2;
			List<String> rrow = new LinkedList<String>();
			for(Object field : row){
				rrow.add(field.toString());
			}
			result.results.add(rrow);
			}
		
		
		return result;
	}
}
