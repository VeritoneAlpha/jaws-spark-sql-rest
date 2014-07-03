package com.xpatterns.jaws.data.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;

import com.xpatterns.jaws.data.DTO.LogDTO;
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO;
import com.xpatterns.jaws.data.DTO.StateDTO;
import com.xpatterns.jaws.data.contracts.IJawsLogging;
import com.xpatterns.jaws.data.utils.JobType;

public class JawsCassandraLogging implements IJawsLogging {

	private static final String CF_SPARK_LOGS = "logs";
	private static final int CF_SPARK_LOGS_NUMBER_OF_ROWS = 100;

	private static final int LEVEL_TYPE = 0;
	private static final int LEVEL_UUID = 1;
	private static final int LEVEL_TIME_STAMP = 2;

	private static final int TYPE_JOB_STATE = -1;
	private static final int TYPE_SCRIPT_DETAILS = 0;
	private static final int TYPE_LOG = 1;
	private static final int TYPE_META = 2;
/*
	private Serializer genericResultSerializer = new Serializer();
*/
	private static Logger logger = Logger.getLogger(JawsCassandraLogging.class.getName());
	private Keyspace keyspace;

	private IntegerSerializer is = IntegerSerializer.get();
	private StringSerializer ss = StringSerializer.get();
	private CompositeSerializer cs = CompositeSerializer.get();
	private LongSerializer ls = LongSerializer.get();
	/*	private LogDTOSerializer logS = LogDTOSerializer.getInstance();*/

	public JawsCassandraLogging() {

	}

	public JawsCassandraLogging(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	@Override
	public void setState(String uuid, JobType type) {

		logger.debug("Writing job state " + type.toString() + " to job " + uuid);

		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.setComponent(LEVEL_TYPE, TYPE_JOB_STATE, is);
		column.setComponent(LEVEL_UUID, uuid, ss);

		Mutator<Integer> mutator = HFactory.createMutator(keyspace, is);
		mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, type.toString(), cs, ss));
		mutator.execute();

	}

	@Override
	public void setScriptDetails(String uuid, String scriptDetails) {

		logger.debug("Writing script details " + scriptDetails + " to job " + uuid);

		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.setComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, is);
		column.setComponent(LEVEL_UUID, uuid, ss);

		Mutator<Integer> mutator = HFactory.createMutator(keyspace, is);
		mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, scriptDetails, cs, ss));
		mutator.execute();

	}

	private int computeRowKey(String uuid) {
		return Math.abs(uuid.hashCode() % CF_SPARK_LOGS_NUMBER_OF_ROWS);
	}

	@Override
	public void addLog(String uuid, String jobId, Long time, String log) {

		logger.debug("Writing log " + log + " to job " + uuid + " at time " + time);
		LogDTO dto = new LogDTO(log, jobId);

		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.setComponent(LEVEL_TYPE, TYPE_LOG, is);
		column.setComponent(LEVEL_UUID, uuid, ss);
		column.setComponent(LEVEL_TIME_STAMP, time, ls);

		Mutator<Integer> mutator = HFactory.createMutator(keyspace, is);
		mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, dto.toJson(), cs, ss));
		mutator.execute();

	}

	@Override
	public JobType getState(String uuid) {

		logger.debug("Reading job state for job: " + uuid);

		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.addComponent(LEVEL_TYPE, TYPE_JOB_STATE, Composite.ComponentEquality.EQUAL);
		column.addComponent(LEVEL_UUID, uuid, Composite.ComponentEquality.EQUAL);

		SliceQuery<Integer, Composite, String> sliceQuery = HFactory.createSliceQuery(keyspace, is, cs, ss);
		sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(column, column, false, 1);

		QueryResult<ColumnSlice<Composite, String>> result = sliceQuery.execute();
		if (result != null) {
			ColumnSlice<Composite, String> columnSlice = result.get();
			if (columnSlice == null) {
				return JobType.NOT_FOUND;
			}
			if (columnSlice.getColumns() == null) {
				return JobType.NOT_FOUND;
			}
			if (columnSlice.getColumns().size() == 0) {
				return JobType.NOT_FOUND;
			}
			HColumn<Composite, String> col = columnSlice.getColumns().get(0);
			String state = col.getValue();

			return JobType.valueOf(state);

		}

		return JobType.NOT_FOUND;
	}

	@Override
	public String getScriptDetails(String uuid) {

		logger.debug("Reading script details for job: " + uuid);

		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.addComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, Composite.ComponentEquality.EQUAL);
		column.addComponent(LEVEL_UUID, uuid, Composite.ComponentEquality.EQUAL);

		SliceQuery<Integer, Composite, String> sliceQuery = HFactory.createSliceQuery(keyspace, is, cs, ss);
		sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(column, column, false, 1);

		QueryResult<ColumnSlice<Composite, String>> result = sliceQuery.execute();
		if (result != null) {
			ColumnSlice<Composite, String> columnSlice = result.get();
			if (columnSlice == null) {
				return "";
			}
			if (columnSlice.getColumns() == null) {
				return "";
			}
			if (columnSlice.getColumns().size() == 0) {
				return "";
			}
			HColumn<Composite, String> col = columnSlice.getColumns().get(0);
			String description = col.getValue();

			return description;

		}

		return "";
	}

	@Override
	public Collection<LogDTO> getLogs(String uuid, Long time, int limit) {

		logger.debug("Reading logs for job: " + uuid + " from date: " + time);
		Collection<LogDTO> logs = new ArrayList<LogDTO>();

		int key = computeRowKey(uuid);

		Composite startColumn = new Composite();
		startColumn.addComponent(LEVEL_TYPE, TYPE_LOG, Composite.ComponentEquality.EQUAL);
		startColumn.addComponent(LEVEL_UUID, uuid, Composite.ComponentEquality.EQUAL);
		startColumn.addComponent(LEVEL_TIME_STAMP, time, Composite.ComponentEquality.EQUAL);

		Composite endColumn = new Composite();
		endColumn.addComponent(LEVEL_TYPE, TYPE_LOG, Composite.ComponentEquality.EQUAL);
		endColumn.addComponent(LEVEL_UUID, uuid, Composite.ComponentEquality.GREATER_THAN_EQUAL);
		SliceQuery<Integer, Composite, String> sliceQuery = HFactory.createSliceQuery(keyspace, is, cs, ss);
		sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(startColumn, endColumn, false, limit);

		QueryResult<ColumnSlice<Composite, String>> result = sliceQuery.execute();
		if (result != null) {
			ColumnSlice<Composite, String> columnSlice = result.get();
			if (columnSlice == null) {
				return logs;
			}
			if (columnSlice.getColumns() == null) {
				return logs;
			}
			if (columnSlice.getColumns().size() == 0) {
				return logs;
			}

			List<HColumn<Composite, String>> columns = columnSlice.getColumns();
			for (HColumn<Composite, String> col : columns) {
				LogDTO log = LogDTO.fromJson(col.getValue());
				Composite colName = col.getName();
				log.timestamp = colName.get(LEVEL_TIME_STAMP, ls);
				logs.add(log);
			}

			return logs;

		}

		return logs;
	}

	@Override
	public Collection<StateDTO> getStateOfJobs(String uuid, int limit) {

		boolean skipFirst = false;
		logger.debug("Reading states for jobs starting with the job: " + uuid);

		TreeMap<String, StateDTO> map = new TreeMap<String, StateDTO>();
		Collection<StateDTO> stateList = new ArrayList<StateDTO>();
		List<Integer> keysList = new ArrayList<Integer>();

		for (int i = 0; i < CF_SPARK_LOGS_NUMBER_OF_ROWS; i++) {
			int key = i;
			keysList.add(key);
		}

		Composite startColumn = new Composite();
		startColumn.addComponent(LEVEL_TYPE, TYPE_JOB_STATE, Composite.ComponentEquality.EQUAL);
		if (uuid != null && !uuid.isEmpty()) {
			startColumn.addComponent(LEVEL_UUID, uuid, Composite.ComponentEquality.EQUAL);
		}

		Composite endColumn = new Composite();
		if (uuid != null && !uuid.isEmpty()) {
			endColumn.addComponent(LEVEL_TYPE, TYPE_JOB_STATE, Composite.ComponentEquality.LESS_THAN_EQUAL);
		} else {
			endColumn.addComponent(LEVEL_TYPE, TYPE_JOB_STATE, Composite.ComponentEquality.GREATER_THAN_EQUAL);
		}

		MultigetSliceQuery<Integer, Composite, String> multiSliceQuery = HFactory.createMultigetSliceQuery(keyspace, is, cs, ss);
		if (uuid != null && !uuid.isEmpty()) {
			multiSliceQuery.setColumnFamily(CF_SPARK_LOGS).setKeys(keysList).setRange(startColumn, endColumn, true, limit + 1);
			skipFirst = true;
		} else {
			multiSliceQuery.setColumnFamily(CF_SPARK_LOGS).setKeys(keysList).setRange(endColumn, startColumn, true, limit);
		}

		QueryResult<Rows<Integer, Composite, String>> result = multiSliceQuery.execute();
		Rows<Integer, Composite, String> rows = result.get();
		if (rows == null || rows.getCount() == 0) {
			return stateList;
		}
		for (Row<Integer, Composite, String> row : rows) {
			ColumnSlice<Composite, String> columnSlice = row.getColumnSlice();
			if (columnSlice == null || columnSlice.getColumns() == null || columnSlice.getColumns().size() == 0) {
				continue;
			}
			List<HColumn<Composite, String>> columns = columnSlice.getColumns();
			for (HColumn<Composite, String> column : columns) {
				Composite name = column.getName();
				if (name.get(LEVEL_TYPE, is) == TYPE_JOB_STATE) {
					// stateList.add(new StateDTO(JobType.valueOf(column.getValue()), name.get(LEVEL_UUID,ss)));
					map.put(name.get(LEVEL_UUID, ss), new StateDTO(JobType.valueOf(column.getValue()), name.get(LEVEL_UUID, ss)));
				}
			}
		}
		return getCollectionFromSortedMapWithLimit(map, limit, skipFirst);
	}

	private Collection<StateDTO> getCollectionFromSortedMapWithLimit(TreeMap<String, StateDTO> map, Integer limit, boolean skipFirst) {

		Collection<StateDTO> collection = new ArrayList<StateDTO>();
		Iterator<String> iterator;

		iterator = map.descendingKeySet().iterator();

		while (iterator.hasNext() && limit > 0) {
			if (skipFirst) {
				skipFirst = false;
				iterator.next();
				continue;
			}
			StateDTO dto = map.get(iterator.next());
			collection.add(dto);
			limit--;
		}

		return collection;
	}

	
	@Override
	public void setMetaInfo(String uuid, ScriptMetaDTO metainfo) throws Exception {
		logger.debug("Writing script meta info " + metainfo + " to job " + uuid);

		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.setComponent(LEVEL_TYPE, TYPE_META, is);
		column.setComponent(LEVEL_UUID, uuid, ss);

		String value = metainfo.toJson();

		Mutator<Integer> mutator = HFactory.createMutator(keyspace, is);

		mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, value, cs, ss));
		mutator.execute();

	}

	@Override
	public ScriptMetaDTO getMetaInfo(String uuid) throws IOException {
		logger.debug("Reading meta info for for job: " + uuid);
		ScriptMetaDTO res = new ScriptMetaDTO();
		int key = computeRowKey(uuid);

		Composite column = new Composite();
		column.addComponent(LEVEL_TYPE, TYPE_META, Composite.ComponentEquality.EQUAL);
		column.addComponent(LEVEL_UUID, uuid, Composite.ComponentEquality.EQUAL);

		ColumnQuery<Integer, Composite, String> columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss);
		columnQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setName(column);
		
		QueryResult<HColumn<Composite, String>> result = columnQuery.execute();
		if (result != null) {
			HColumn<Composite, String> col = result.get();
			
			if (col == null) {
				return res;
			}
			
			res = ScriptMetaDTO.fromJson(col.getValue());
		}

		return res;
	}

}
