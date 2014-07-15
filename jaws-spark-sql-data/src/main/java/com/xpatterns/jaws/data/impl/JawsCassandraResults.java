package com.xpatterns.jaws.data.impl;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;

import com.xpatterns.jaws.data.DTO.ResultDTO;
import com.xpatterns.jaws.data.contracts.IJawsResults;

public class JawsCassandraResults implements IJawsResults {

    private static final String CF_SPARK_RESULTS = "results";
    private static final int CF_SPARK_RESULTS_NUMBER_OF_ROWS = 100;

    private static Logger logger = Logger.getLogger(JawsCassandraResults.class.getName());
    private Keyspace keyspace;
    
    private IntegerSerializer is = IntegerSerializer.get();
    private StringSerializer ss = StringSerializer.get();
    public JawsCassandraResults(){
    	
    }
    
    public JawsCassandraResults(Keyspace keyspace){
    	this.keyspace = keyspace;
    }
	
	
	
	@Override
	public ResultDTO getResults(String uuid) {
		

		logger.debug("Reading results for query: " + uuid);

		int key = Math.abs(uuid.hashCode() % CF_SPARK_RESULTS_NUMBER_OF_ROWS);
		
		SliceQuery<Integer, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, is, ss, ss);
		sliceQuery.setColumnFamily(CF_SPARK_RESULTS).setKey(key).setRange(uuid, uuid, false, 1);
		
		QueryResult<ColumnSlice<String,String>> result = sliceQuery.execute();
		if (result != null) {
		    ColumnSlice<String,String> columnSlice = result.get();
		    if (columnSlice == null) {
		    	return null;
		    }
		    if (columnSlice.getColumns() == null) {
		    	return null;
		    }
		    if (columnSlice.getColumns().size() == 0) {
		    	return null;
		    }
		
		    HColumn<String,String> col = columnSlice.getColumns().get(0);
		    ResultDTO dto = ResultDTO.fromJson(col.getValue());

		    return dto;

		}
		
		return null;
		
	}

	@Override
	public void setResults(String uuid, ResultDTO resultDTO) {
	
		logger.debug("Writing results to query " + uuid);

		int key = Math.abs(uuid.hashCode() % CF_SPARK_RESULTS_NUMBER_OF_ROWS);
		
		String buffer = resultDTO.toJson();
		
		Mutator<Integer> mutator = HFactory.createMutator(keyspace, is);
		mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(uuid, buffer, ss, ss));
		mutator.execute();
		
	}

}
