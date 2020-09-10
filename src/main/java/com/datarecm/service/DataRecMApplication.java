package com.datarecm.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.datarecm.service.glue.datacatalog.util.TableWithPartitions;
import com.google.gson.Gson;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain, Amazon Web Services, Inc.
 *
 */
@SpringBootApplication
public class DataRecMApplication {
	public static GlueService glueService = new GlueService();

	public static void main(String[] args) {
		//SpringApplication.run(DataRecMApplication.class, args);
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.AP_SOUTH_1.getName());
		String sourceGlueCatalogId = Optional.ofNullable(System.getenv("source_glue_catalog_id")).orElse("436386478328");
		String dbPrefixString = Optional.ofNullable(System.getenv("database_prefix_list")).orElse("");
		String separator = Optional.ofNullable(System.getenv("separator")).orElse("|");
		//String topicArn = Optional.ofNullable(System.getenv("sns_topic_arn_gdc_replication_planner")).orElse("arn:aws:sns:ap-south-1:436386478328:GlueExportSNSTopic");
		String ddbTblNameForDBStatusTracking = Optional.ofNullable(System.getenv("ddb_name_gdc_replication_planner"))
				.orElse("ddb_name_gdc_replication_planner");

		// Print environment variables
		printEnvVariables(sourceGlueCatalogId, null, ddbTblNameForDBStatusTracking, dbPrefixString, separator);
		// Create Objects for Glue
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).build();

		Database glueDB = glueService.selectDB(sourceGlueCatalogId,glue);

		Table table= glueService.selectTable(sourceGlueCatalogId, glueDB, glue);
		List<Partition> partitionList = glueService.getGlueUtil().getPartitions(glue, sourceGlueCatalogId, table.getDatabaseName(), table.getName());
		System.out.printf("\nDatabase: '%s', Table: %s, num_partitions: %d \n", table.getDatabaseName(), table.getName(), partitionList.size());

		Map<String, String> map= table.getParameters();
		StorageDescriptor tableDesc = table.getStorageDescriptor();
		List<Column> columns= tableDesc.getColumns();
		System.out.printf("\ncolumns: %d \n", columns.size());
		//System.out.printf("\ncolumns: %d \n", map.get));

		for (Column column : columns) {
		//	System.out.println(column.getName() +":"+column.getType());
			System.out.println(column.toString());

		}
		//System.out.println(columns.toString());
		
		
		//Gson gson = new Gson();
		// Convert Table to JSON String
		//String tableDDL = gson.toJson(table);
		//System.out.println(new AttributeValue().withS(tableDDL));

//		TableWithPartitions tableWithParts = new TableWithPartitions();
//		tableWithParts.setPartitionList(partitionList);
//		tableWithParts.setTable(table);
//		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
//		item.put("table_id", new AttributeValue().withS(table.getName().concat("|").concat(table.getDatabaseName())));
//		item.put("source_glue_catalog_id", new AttributeValue().withS(sourceGlueCatalogId));
//		item.put("table_schema", new AttributeValue().withS(tableDDL)); 
//		item.put("is_large_table", new AttributeValue().withS(Boolean.toString(false)));


	}

	/**
	 * Tokenize the Data Prefix String to a List of Prefixes
	 * @param dbPrefixString
	 * @param token
	 * @return
	 */
	public static List<String> tokenizeDatabasePrefixString(String str, String separator) {

		List<String> dbPrefixesList = Collections.list(new StringTokenizer(str, separator)).stream()
				.map(token -> (String) token)
				.collect(Collectors.toList());
		System.out.println("Number of database prefixes: " + dbPrefixesList.size());
		return dbPrefixesList;
	}

	/**
	 * 
	 * @param dBList
	 * @param requiredDBPrefixList
	 * @return
	 */
	public static List<Database> getRequiredDatabases(List<Database> dBList, List<String> dbPrefixesList){

		List<Database> dBsToExportList = new ArrayList<Database>();
		for(Database database : dBList) {
			for(String dbPrefix : dbPrefixesList) {
				if(database.getName().toLowerCase().startsWith(dbPrefix)) {
					dBsToExportList.add(database);
					break;
				}
			}
		}
		System.out.printf("Number of databases in Glue Catalog: %d, number of databases to be exported: %d \n", dBList.size(), dBsToExportList.size());
		return dBsToExportList;
	}
	/**
	 * This method prints environment variables
	 * @param sourceGlueCatalogId
	 * @param topicArn
	 * @param ddbTblNameForDBStatusTracking
	 */
	public static void printEnvVariables(String sourceGlueCatalogId, String topicArn,
			String ddbTblNameForDBStatusTracking, String dbPrefixString, String separator) {
		System.out.println("SNS Topic Arn: " + topicArn);
		System.out.println("Source Catalog Id: " + sourceGlueCatalogId);
		System.out.println("Database Prefix String: " + dbPrefixString);
		System.out.println("Prefix Separator: " + separator);
		System.out.println("DynamoDB Table to track GDC Replication Planning: " + ddbTblNameForDBStatusTracking);

	}

}
