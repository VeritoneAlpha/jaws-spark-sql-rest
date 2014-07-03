package com.xpatterns.jaws.data.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class Utils {
	private static final String NAMENODE = "namenode";
	public static final String FORCED_MODE = "forcedMode";
	public static final String LOGGING_FOLDER = "loggingFolder";
	public static final String STATUS_FOLDER = "stateFolder";
	public static final String METAINFO_FOLDER = "metaInfoFolder";
	public static final String DETAILS_FOLDER = "detailsFolder";
	public static final String RESULTS_FOLDER = "resultsFolder";
	public static final String SCHEMA_FOLDER = "schemaFolder";
	public static final String REPLICATION_FACTOR = "replicationFactor";

	private static Logger log = Logger.getLogger(Utils.class.getName());

	public static boolean areTheSame(Object first, Object second) {
		if (first == null) {
			if (second != null)
				return false;
		} else if (!first.equals(second))
			return false;
		return true;
	}

	public static boolean areTheSameDates(DateTime first, DateTime second) {
		if (first == null) {
			if (second != null)
				return false;
		} else if (!first.isEqual(second))
			return false;
		return true;
	}

	public static Properties getSettings(InputStream inputStream) throws Exception {
		Properties p = new Properties();
		try {
			try {
				p.load(inputStream);
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}

		return p;
	}

	public static Configuration getConfiguration(Properties properties) {

		String namenodeEndpoint = properties.getProperty(Utils.NAMENODE);
		String replicationFactor = properties.getProperty(Utils.REPLICATION_FACTOR);
		Boolean forcedMode = Boolean.parseBoolean(properties.getProperty(Utils.FORCED_MODE, "false"));

		Configuration configuration = new Configuration();
		configuration.setBoolean(Utils.FORCED_MODE, forcedMode);

		// set hadoop name node and job tracker
		configuration.set("fs.defaultFS", namenodeEndpoint);
		configuration.set("dfs.replication", replicationFactor);

		return configuration;
	}

	public static void createFolderIfDoesntExist(Configuration configuration, String folder, boolean forcedMode) throws IOException {
		log.info("[createFolderIfDoesntExist] forcedMode: " + forcedMode);

		FileSystem fs = null;
		try {
			fs = FileSystem.newInstance(configuration);
			Path path = new Path(folder);
			if (fs.exists(path)) {
				log.info("[createFolderIfDoesntExist] The folder " + folder + " exists!");
				if (forcedMode) {
					log.info("[createFolderIfDoesntExist] We are in forced mode so the folder will be recreated");
					// Delete folder
					fs.delete(path, true);
					createFolder(fs, folder);
				} else {
					log.info("[createFolderIfDoesntExist] We won't recreate the folder!");
				}
			} else {
				createFolder(fs, folder);
			}

		} catch (IOException e) {
			log.error(e.getMessage());
			throw e;
		} finally {
			if (fs != null) {
				fs.close();
			}

		}
	}

	public static void createFolder(FileSystem fs, String path) throws IOException {
		Path temPath = new Path(path);
		if (!fs.exists(temPath))
			fs.mkdirs(temPath);
	}

	public static void appendToFile(String message, Configuration configuration, String filename) throws Exception, IOException {

		InputStream in = new BufferedInputStream(new ByteArrayInputStream(message.getBytes()));
		Path file = new Path(filename);
		FileSystem fs = FileSystem.newInstance(configuration);

		try {
			FSDataOutputStream fileOutputStream = null;

			if (fs.exists(file)) {
				fileOutputStream = fs.append(file);
			} else {
				fileOutputStream = fs.create(file, false);
			}

			IOUtils.copyBytes(in, fileOutputStream, configuration, true);
		} finally {
			fs.close();
		}

	}

	public static void rewriteFile(String message, Configuration configuration, String filename) throws Exception, IOException {
		rewriteFile(message.getBytes(), configuration, filename);
	}
	
	public static void rewriteFile(byte[] message, Configuration configuration, String filename) throws Exception, IOException {
		FileSystem fs = null;
		InputStream in = new BufferedInputStream(new ByteArrayInputStream(message));
		String temporaryFileName = filename + "_temp";

		try {
			// failover mechanism
			Path file = new Path(filename);
			Path temporaryFile = new Path(temporaryFileName);

			fs = FileSystem.newInstance(configuration);
			FSDataOutputStream fileOutputStream = fs.create(temporaryFile, true);

			// write into temporary file
			IOUtils.copyBytes(in, fileOutputStream, configuration, true);

			// delete the old file and rename the temporary file
			if (fs.exists(file)) {
				fs.delete(file, true);
			}
			fs.rename(temporaryFile, file);
		} catch (Exception ex) {
			log.error(ex.getMessage());
			throw ex;
		} finally {
			if (fs != null) {
				fs.close();
			}

		}
	}

	public static String readFile(Configuration configuration, String filename) throws IOException {
		StringBuilder content = new StringBuilder();
		BufferedReader br = null;
		FileSystem fs = null;
		try {
			Path filePath = new Path(filename);
			fs = FileSystem.get(configuration);
			br = new BufferedReader(new InputStreamReader(fs.open(filePath)));

			String line = br.readLine();
			while (line != null) {
				content.append(line + "\n");
				line = br.readLine();
			}
			return content.toString().trim();
		} finally {
			if (br != null) {
				br.close();
			}
			if (fs != null) {
				fs.close();
			}
		}

	}
	
	public static byte[] readBytesFromFile(Configuration configuration, String filename) throws IOException {
		FileSystem fs = null;
		try {
			Path filePath = new Path(filename);
			fs = FileSystem.get(configuration);
			FSDataInputStream is = fs.open(filePath);
			byte[] bytes = new byte[is.available()];
			is.read(bytes);
			
			
			return bytes;
		} finally {
			if (fs != null) {
				fs.close();
			}
		}

	}

	public static String getNameFromPath(String path) {
		String name = "";
		if (path.charAt(path.length() - 1) == '/')
			path = path.substring(0, path.length() - 1);

		if (path.contains("/"))
			name = path.substring(path.lastIndexOf("/") + 1, path.length());
		else
			name = path;
		return name;
	}

	public static SortedSet<String> listFiles(Configuration configuration, String folderName, Comparator<String> comparator) throws IOException {
		FileSystem fs = null;
		SortedSet<String> allFiles = new TreeSet<String>(comparator);
		try {
			Path folderPath = new Path(folderName);
			fs = FileSystem.get(configuration);
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(folderPath, false);
			while (files.hasNext()) {
				LocatedFileStatus file = files.next();
				allFiles.add(file.getPath().getName());
			}

			return allFiles;

		} finally {
			if (fs != null) {
				fs.close();
			}
		}
	}
}
