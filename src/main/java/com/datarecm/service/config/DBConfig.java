package com.datarecm.service.config;

import java.util.List;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Holding all configs
 * @author Punit Jain
 *
 */
public class DBConfig {
	private int printUnmatchedRecordCount;
	private String hostname;
	private int port;
	private String username;
	private String password;
	private String dbname;
	@NotBlank
	private String dbtype;
	private String primaryKey;
	@NotBlank
	private String tableName;
	private String tableSchema;		
	@NotBlank
	private String region;
	private boolean evaluateDataRules;
	private String url;
	private String atheneOutputDir;

	private List<String> ignoreList;
	
	public String getAccessKey() {
		 
		return (dbtype+dbname+tableSchema+tableName+primaryKey).toLowerCase();
	}


	public int getPrintUnmatchedRecordCount() {
		return printUnmatchedRecordCount;
	}


	public void setPrintUnmatchedRecordCount(int printUnmatchedRecordCount) {
		this.printUnmatchedRecordCount = printUnmatchedRecordCount;
	}


	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public List<String> getIgnoreList() {
		return ignoreList;
	}

	public void setIgnoreList(List<String> ignoreList) {
		this.ignoreList = ignoreList;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getDbtype() {
		return dbtype;
	}

	public void setDbtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public boolean isEvaluateDataRules() {
		return evaluateDataRules;
	}

	public void setEvaluateDataRules(boolean evaluateDataRules) {
		this.evaluateDataRules = evaluateDataRules;
	}

	public String getAtheneOutputDir() {
		return atheneOutputDir;
	}

	public void setAtheneOutputDir(String atheneOutputDir) {
		this.atheneOutputDir = atheneOutputDir;
	}



	/*
	public void loadSourceConfig() throws IOException {
		try {
			Properties prop = new Properties();
			String propFileName = "application.properties";

			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			Date time = new Date(System.currentTimeMillis());
			//System.out.println(prop.toString());
			sourceConfig = new SourceConfig();
			// get the property value and print it out
			sourceConfig.setHostname(prop.getProperty("source.hostname"));
			sourceConfig.setUrl(prop.getProperty("source.url"));
			sourceConfig.setPort(Integer.parseInt(prop.getProperty("source.port")));
			sourceConfig.setPassword(prop.getProperty("source.password"));
			sourceConfig.setDbname(prop.getProperty("source.dbname"));
			sourceConfig.setDbtype(prop.getProperty("source.dbtype"));
			sourceConfig.setUsername(prop.getProperty("source.username"));
			sourceConfig.setRule1(prop.getProperty("source.rule1"));

			System.out.println( "\nProgram Ran on " + time + " by user=" +sourceConfig.getUsername() );
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
	}
	 */

}
