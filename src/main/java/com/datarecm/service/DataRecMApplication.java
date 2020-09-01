package com.datarecm.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.gdcreplication.util.DDBUtil;
import com.amazonaws.gdcreplication.util.GlueUtil;
import com.amazonaws.gdcreplication.util.SNSUtil;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

@SpringBootApplication
public class DataRecMApplication {

	public static void main(String[] args) {
		//SpringApplication.run(DataRecMApplication.class, args);

		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String sourceGlueCatalogId = Optional.ofNullable(System.getenv("source_glue_catalog_id")).orElse("1234567890");
		String dbPrefixString = Optional.ofNullable(System.getenv("database_prefix_list")).orElse("");
		String separator = Optional.ofNullable(System.getenv("separator")).orElse("|");
		String topicArn = Optional.ofNullable(System.getenv("sns_topic_arn_gdc_replication_planner"))
				.orElse("arn:aws:sns:us-east-1:1234567890:GlueExportSNSTopic");
		String ddbTblNameForDBStatusTracking = Optional.ofNullable(System.getenv("ddb_name_gdc_replication_planner"))
				.orElse("ddb_name_gdc_replication_planner");

		// Print environment variables
		printEnvVariables(sourceGlueCatalogId, topicArn, ddbTblNameForDBStatusTracking, dbPrefixString, separator);

		// Create Objects for Glue and SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).build();
		AmazonSNS sns = AmazonSNSClientBuilder.standard().withRegion(region).build();

		// Create Objects for Utility classes
		DDBUtil ddbUtil = new DDBUtil();
		SNSUtil snsUtil = new SNSUtil();
		GlueUtil glueUtil = new GlueUtil();

		// Get databases from Glue
		int numberOfDatabasesExported = 0;
		List<Database> dBList = glueUtil.getDatabases(glue, sourceGlueCatalogId);

		// When database Prefix string is empty or not provided then, it imports all databases
		// else, it imports only the databases that has the same prefix
		if (dbPrefixString.equalsIgnoreCase("")) {
			numberOfDatabasesExported = snsUtil.publishDatabaseSchemasToSNS(sns, dBList, topicArn, ddbUtil,
					ddbTblNameForDBStatusTracking, sourceGlueCatalogId);
		} else {
			// Tokenize the database prefix string to a List of database prefixes
			List<String> dbPrefixList = tokenizeDatabasePrefixString(dbPrefixString, separator);
			// Identify required databases to export
			List<Database> dBsListToExport = getRequiredDatabases(dBList, dbPrefixList);
			// Publish schemas for databases to SNS Topic
			numberOfDatabasesExported = snsUtil.publishDatabaseSchemasToSNS(sns, dBsListToExport, topicArn, ddbUtil,
					ddbTblNameForDBStatusTracking, sourceGlueCatalogId);
		}
		System.out.printf(
				"Database export statistics: number of databases exist = %d, number of databases exported to SNS = %d. \n",
				dBList.size(), numberOfDatabasesExported);
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
