package com.xpatterns.jaws.data.impl;


import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.xpatterns.jaws.data.DTO.LogDTO;
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO;
import com.xpatterns.jaws.data.DTO.StateDTO;
import com.xpatterns.jaws.data.contracts.IJawsLogging;
import com.xpatterns.jaws.data.utils.JobType;
import com.xpatterns.jaws.data.utils.Randomizer;

public class JawsLoggingTest {

	private static IJawsLogging dal;

	@BeforeClass
	public static void setUp() throws Exception {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"test-application-context.xml");
		dal = (IJawsLogging) context.getBean("dal");

	}

	@Test
	public void testWriteReadStatus() throws Exception {
		String uuid = DateTime.now().toString();
		dal.setState(uuid, JobType.IN_PROGRESS);
		JobType state = dal.getState(uuid);
		Assert.assertEquals(JobType.IN_PROGRESS, state);
		
		dal.setState(uuid, JobType.DONE);
		state = dal.getState(uuid);
		Assert.assertEquals(JobType.DONE, state);
	}
	
	@Test
	public void testWriteReadMetaInfo() throws Exception {
		String uuid = DateTime.now().toString();
		ScriptMetaDTO metaInfo = Randomizer.createScriptMetaDTO();
		dal.setMetaInfo(uuid, metaInfo);
		ScriptMetaDTO result = dal.getMetaInfo(uuid);
		
		Assert.assertEquals(metaInfo, result);
	}
	
	@Test
	public void testWriteReadDetails() throws Exception {
		String uuid = DateTime.now().toString();
		String details = Randomizer.getRandomString(10);
		
		dal.setScriptDetails(uuid, details);
		String resultDetails = dal.getScriptDetails(uuid);
		Assert.assertEquals(details, resultDetails);
	}
	
	@Test
	public void testWriteReadLogs() throws IOException, Exception {
		String uuid = DateTime.now().toString();
		String jobId = Randomizer.getRandomString(5);
		String log = Randomizer.getRandomString(300);
		Long now = System.currentTimeMillis();
		LogDTO logDto = new LogDTO(log, jobId);
		logDto.timestamp = now;
		
		dal.addLog(uuid, jobId, now, log);
		Collection<LogDTO> result = dal.getLogs(uuid, now, 100);
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(logDto, result.iterator().next());
		 
		dal.addLog(uuid, jobId, System.currentTimeMillis() + 100, log);
		dal.addLog(uuid, jobId, System.currentTimeMillis() + 200, log);
		dal.addLog(uuid, jobId, System.currentTimeMillis() + 300, log);
		
		result = dal.getLogs(uuid, now, 100);
		Assert.assertEquals(4, result.size());
		
		result = dal.getLogs(uuid, now, 2);
		Assert.assertEquals(2, result.size());
	}
	
	@Test
	public void testWriteReadStates() throws IOException, Exception {
		String uuid = DateTime.now() + " - 1";
		Thread.sleep(300);
		String uuid2 = DateTime.now() + " - 2";
		Thread.sleep(300);
		String uuid3 = DateTime.now() + " - 3";
		Thread.sleep(300);
		String uuid4 = DateTime.now() + " - 4";
		Thread.sleep(300);
		String uuid5 = DateTime.now() + " - 5";
		Thread.sleep(300);
		String uuid6 = DateTime.now() + " - 6";
		String jobId = Randomizer.getRandomString(5);
		String log = Randomizer.getRandomString(300);
		Long now = System.currentTimeMillis();
		LogDTO logDto = new LogDTO(log, jobId);
		logDto.timestamp = now;
		
		dal.addLog(uuid, jobId, now, log);
		dal.addLog(uuid2, jobId, now, log);
		dal.addLog(uuid3, jobId, now, log);
				
		dal.setState(uuid, JobType.DONE);
		dal.setState(uuid2, JobType.IN_PROGRESS);
		dal.setState(uuid3, JobType.FAILED);
		dal.setState(uuid4, JobType.FAILED);
		dal.setState(uuid5, JobType.FAILED);
		dal.setState(uuid6, JobType.FAILED);
		
		Collection<StateDTO> stateOfJobs = dal.getStateOfJobs(null, 3);
		System.out.println(stateOfJobs);
		Assert.assertEquals(3, stateOfJobs.size());
		Iterator<StateDTO> iterator = stateOfJobs.iterator();
		Assert.assertEquals(uuid6,iterator.next().uuid);
		Assert.assertEquals(uuid5,iterator.next().uuid);
		Assert.assertEquals(uuid4,iterator.next().uuid);	
		
		stateOfJobs = dal.getStateOfJobs(uuid4, 3);
		System.out.println(stateOfJobs);
		Assert.assertEquals(3, stateOfJobs.size());
		iterator = stateOfJobs.iterator();
		Assert.assertEquals(uuid3,iterator.next().uuid);
		Assert.assertEquals(uuid2,iterator.next().uuid);
		Assert.assertEquals(uuid,iterator.next().uuid);
		
		
		stateOfJobs = dal.getStateOfJobs(uuid3, 2);
		System.out.println(stateOfJobs);
		Assert.assertEquals(2, stateOfJobs.size());
		iterator = stateOfJobs.iterator();
		Assert.assertEquals(uuid2,iterator.next().uuid);
		Assert.assertEquals(uuid,iterator.next().uuid);
		
		
	}

}
