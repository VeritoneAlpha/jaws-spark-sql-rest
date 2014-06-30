package com.xpatterns.jaws.data.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.xpatterns.jaws.data.DTO.ResultDTO;
import com.xpatterns.jaws.data.contracts.IJawsResults;
import com.xpatterns.jaws.data.utils.Utils;

public class JawsHdfsResults implements IJawsResults {

	Configuration configuration;
	private static Logger logger = Logger.getLogger(JawsHdfsResults.class.getName());

	public JawsHdfsResults() {

	}

	public JawsHdfsResults(Configuration configuration) throws Exception {
		// getting the properties from the properties file

		this.configuration = configuration;
		boolean forcedMode = configuration.getBoolean(Utils.FORCED_MODE, false);

		Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.RESULTS_FOLDER), forcedMode);
	}


	@Override
	public void setResults(String uuid, ResultDTO resultDTO) throws IOException, Exception {

		logger.info("Writing results to job " + uuid);
		Utils.rewriteFile(resultDTO.toJson().getBytes(), configuration, configuration.get(Utils.RESULTS_FOLDER) + "/" + uuid);

	}
	
	@Override
	public ResultDTO getResults(String uuid) throws IOException {
		logger.debug("Reading results for job: " + uuid);
		FileSystem fs = null;
		ByteArrayOutputStream fileContent = new ByteArrayOutputStream();
		try {
			Path filePath = new Path(configuration.get(Utils.RESULTS_FOLDER) + "/" + uuid);
			fs = FileSystem.get(configuration);
			InputStream in = null;
			try{
				in = fs.open(filePath);
				IOUtils.copyBytes(in, fileContent, configuration);
				return ResultDTO.fromJson(new String(fileContent.toByteArray()));
			}
			finally{
				IOUtils.closeStream(in);
			}
			
		}
		finally{
			fs.close();
		}
	
	}

	
}
