package com.datarecm.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.model.Database;

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
		glueService.glueOperation(region, sourceGlueCatalogId);

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
