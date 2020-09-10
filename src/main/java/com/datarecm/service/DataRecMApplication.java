package com.datarecm.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.amazonaws.regions.Regions;
import com.datarecm.service.config.SourceConfig;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain, Amazon Web Services, Inc.
 *
 */


@EnableConfigurationProperties(SourceConfig.class)
@SpringBootApplication
public class DataRecMApplication {
	
	public static GlueService glueService = new GlueService();
	public static AthenaService athenaService= new AthenaService();
	@Autowired
	private SourceConfig sourceConfig;
	InputStream inputStream;
	SourceConnection sourceDB = new SourceConnection();

	@PostConstruct
	public void init() throws Exception {
		System.out.println("************************8");
		System.out.println(sourceConfig.getHostname());
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException {

		//SpringApplication.run(DataRecMApplication.class, args);
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.AP_SOUTH_1.getName());
		String sourceGlueCatalogId = Optional.ofNullable(System.getenv("source_glue_catalog_id")).orElse("436386478328");
		String dbPrefixString = Optional.ofNullable(System.getenv("database_prefix_list")).orElse("");
		String separator = Optional.ofNullable(System.getenv("separator")).orElse("|");
		//String topicArn = Optional.ofNullable(System.getenv("sns_topic_arn_gdc_replication_planner")).orElse("arn:aws:sns:ap-south-1:436386478328:GlueExportSNSTopic");
		// Print environment variables
		printEnvVariables(sourceGlueCatalogId, null, dbPrefixString, separator);
		//glueService.glueOperation(region, sourceGlueCatalogId);
		
		
		DataRecMApplication app = new DataRecMApplication();
		app.loadSourceConfig();
		//app.sourceDB.
		//TableCompare tc = new TableCompare();
		//tc.compareTables();
		app.runRule(app.sourceConfig.getRule1());
		athenaService.runQueries();
	}

	/**
	 * This method prints environment variables
	 * @param sourceGlueCatalogId
	 * @param topicArn
	 * @param ddbTblNameForDBStatusTracking
	 */
	public static void printEnvVariables(String sourceGlueCatalogId, String topicArn, String dbPrefixString, String separator) {
		System.out.println("SNS Topic Arn: " + topicArn);
		System.out.println("Source Catalog Id: " + sourceGlueCatalogId);
		System.out.println("Database Prefix String: " + dbPrefixString);
		System.out.println("Prefix Separator: " + separator);

	}


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

			sourceDB.getConnection(sourceConfig);

			System.out.println( "\nProgram Ran on " + time + " by user=" +sourceConfig.getUsername() );
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
	}

	private void runRule(String rule1) throws SQLException, ClassNotFoundException{
		String rule11="select count(*) from dms_sample.\"order\";";
		String rul2= "select ordinal_position as \"colum_position\",column_name,\n" + 
		"    case \n" + 
		"      when data_type= 'timestamp without time zone' then 'timestamp' \n" + 
		"      when data_type= 'double precision' then 'float8' \n" + 
		"      when data_type= 'character varying' then 'varchar' \n" + 
		"      else data_type \n" + 
		"    END \n" + 
		"    FROM information_schema.columns \n" + 
		"    WHERE table_name = 'order' \n" + 
		"    ORDER BY ordinal_position;\n" + 
		";";
		if(sourceDB.getConnection()!=null){
			PreparedStatement ruleStatement = sourceDB.getConnection().prepareStatement(rule11);
			ResultSet resultSet = ruleStatement.executeQuery();			
			ResultSetMetaData rsmd = resultSet.getMetaData();
			   System.out.println("querying result");
			   int columnsNumber = rsmd.getColumnCount();
			   while (resultSet.next()) {
			       for (int i = 1; i <= columnsNumber; i++) {
			           if (i > 1) System.out.print(",  ");
			           String columnValue = resultSet.getString(i);
			           System.out.print(columnValue + " " + rsmd.getColumnName(i));
			       }
			       System.out.println("");
			   }
//			sourceRS.
			//System.out.println(sourceRS.toString());

			ruleStatement.close();
		}
	}

}
