package com.xpatterns.jaws.data.impl;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.xpatterns.jaws.data.DTO.LogDTO;
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO;
import com.xpatterns.jaws.data.DTO.StateDTO;
import com.xpatterns.jaws.data.contracts.IJawsLogging;
import com.xpatterns.jaws.data.utils.JobType;
import com.xpatterns.jaws.data.utils.Randomizer;

public class JawsLoggingOnHdfsTest {
	private static IJawsLogging dal;

	@Before
	public void setUp() throws Exception {
		ApplicationContext context = new ClassPathXmlApplicationContext("test-application-context-hdfs.xml");
		dal = (IJawsLogging) context.getBean("dal");

	}


	@Test
	public void testWriteReadMeta_1() throws Exception {
		String uuid = Randomizer.getRandomString(10);
		ScriptMetaDTO dt = Randomizer.createScriptMetaDTO();
	
		dal.setMetaInfo(uuid, dt);
		ScriptMetaDTO result = dal.getMetaInfo(uuid);
		
		Assert.assertEquals(dt, result);
	}
	
	@Test
	public void testWriteReadStatus_1() throws Exception {
		String uuid = Randomizer.getRandomString(10);
		JobType type = JobType.FAILED;
	
		dal.setState(uuid, type);
		JobType resultType = dal.getState(uuid);
		
		Assert.assertEquals(type, resultType);
	}
	
	@Test
	public void testWriteReadStatus_2() throws Exception {
		String uuid1 = "123";
		String uuid2 = "124";
		JobType type1 = JobType.FAILED;
		JobType type2 = JobType.FAILED;
	
		dal.setState(uuid1, type1);
		dal.setState(uuid2, type2);
		
		Collection<StateDTO> result1 = dal.getStateOfJobs(uuid1, 2);
		
		Assert.assertEquals(2, result1.size());
		Iterator<StateDTO> it = result1.iterator();
		
		StateDTO state2 = it.next();
		StateDTO state1 = it.next();
		
		Assert.assertEquals(type1, state1.state);
		Assert.assertEquals(uuid1, state1.uuid);
		
		Assert.assertEquals(type2, state2.state);
		Assert.assertEquals(uuid2, state2.uuid);
		
		Collection<StateDTO> result2 = dal.getStateOfJobs(uuid1, 1);
		
		Assert.assertEquals(1, result2.size());
		Iterator<StateDTO> it2 = result2.iterator();
		
		StateDTO state3 = it2.next();
		Assert.assertEquals(state2, state3);
		
		Collection<StateDTO> result3 = dal.getStateOfJobs(uuid1, 3);
		
		Assert.assertEquals(2, result3.size());
		Iterator<StateDTO> it3 = result3.iterator();
		
		StateDTO state4 = it3.next();
		StateDTO state5 = it3.next();
		
		Assert.assertEquals(state2, state4);
		Assert.assertEquals(state1, state5);

	}
	
	@Test
	public void testWriteReadDetails() throws Exception {
		String uuid = Randomizer.getRandomString(10);
		String scriptDetails = Randomizer.getRandomString(100);
	
		dal.setScriptDetails(uuid, scriptDetails);
		String resultDetails = dal.getScriptDetails(uuid);
		
		Assert.assertEquals(scriptDetails, resultDetails);
	}
	
	@Test
	public void testWriteReadLogs_1() throws Exception {
		String uuid = Randomizer.getRandomString(10);
		String jobId = Randomizer.getRandomString(10);
		long time = Randomizer.getRandomLong();
		String log = Randomizer.getRandomString(100);
	
		dal.addLog(uuid, jobId, time, log);
		Collection<LogDTO> result = dal.getLogs(uuid, time, 1);
		
		Assert.assertEquals(1, result.size());
		LogDTO resultLog = result.iterator().next();
		Assert.assertEquals(log, resultLog.log);
		Assert.assertEquals(jobId, resultLog.jobId);
		Assert.assertEquals(time, resultLog.timestamp);
		
	}
	
	@Test
	public void testWriteReadLogs_2() throws Exception {
		String uuid = Randomizer.getRandomString(10);
		String jobId = Randomizer.getRandomString(10);
		long time = 123;
		String log = Randomizer.getRandomString(100);
		
		long time2 = 124;
		String log2 = Randomizer.getRandomString(100);
	
		dal.addLog(uuid, jobId, time, log);
		dal.addLog(uuid, jobId, time2, log2);
		Collection<LogDTO> result = dal.getLogs(uuid, time, 2);
		Collection<LogDTO> result2 = dal.getLogs(uuid, time, 1);
		
		Assert.assertEquals(2, result.size());
		Iterator<LogDTO> it = result.iterator();
		LogDTO resultLog1 = it.next();
		LogDTO resultLog12 = it.next();
		Assert.assertEquals(log, resultLog1.log);
		Assert.assertEquals(jobId, resultLog1.jobId);
		Assert.assertEquals(time, resultLog1.timestamp);
		
		Assert.assertEquals(log2, resultLog12.log);
		Assert.assertEquals(jobId, resultLog12.jobId);
		Assert.assertEquals(time2, resultLog12.timestamp);
		
		Assert.assertEquals(1, result2.size());
		Iterator<LogDTO> it2 = result2.iterator();
		LogDTO resultLog3 = it2.next();
		
		Assert.assertEquals(resultLog1, resultLog3);
		
	}
}
