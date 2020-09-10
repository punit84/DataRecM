package com.datarecm.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.datarecm.service.glue.datacatalog.util.GlueUtil;
import com.datarecm.service.glue.datacatalog.util.TableWithPartitions;
import com.google.gson.Gson;

/**
 * @author jainpuni
 *
 */
public class GlueService {
	private GlueUtil glueUtil = new GlueUtil();

	public GlueUtil getGlueUtil() {
		return glueUtil;
	}


	public Database selectDB(String sourceGlueCatalogId,AWSGlue glue) {

		// Get databases from Glue

		List<Database> dBList = glueUtil.getDatabases(glue, sourceGlueCatalogId);
		System.out.println("***********************************");

		System.out.println("Choose DB from following:");

		for (int i = 0; i < dBList.size(); i++) {

			System.out.println(i+1 +" : " + dBList.get(i).getName());

		}
		//String inputString = stdIn.nextLine();
		Database destDB=null;

		int key = takeKeyInput(dBList.size());
		destDB = dBList.get(key-1);
		System.out.println("Selected DB as " + destDB.getName());

		return destDB;

	}
	
	public Table selectTable(String sourceGlueCatalogId,Database db,AWSGlue glue) {

		List<Table> dbTableList = glueUtil.getTables(glue, sourceGlueCatalogId, db.getName());
		
		System.out.println("Choose Tables from following:");

		for (int i = 0; i < dbTableList.size(); i++) {

			System.out.println(i+1 +" : " + dbTableList.get(i).getName());

		}
		//String inputString = stdIn.nextLine();
		Table table=null;

		int key = takeKeyInput(dbTableList.size());
		table = dbTableList.get(key-1);
		System.out.println("Selected Table as " + table.getName());

		return table;

	}
	
	public List<Map<String, AttributeValue>> getAllDB(String sourceGlueCatalogId,AWSGlue glue) {

		// Create Objects for Utility classes
		//DDBUtil ddbUtil = new DDBUtil();
		//SNSUtil snsUtil = new SNSUtil();
		//int numberOfDatabasesExported = 0;
		//List<Database> dBsToExportList = new ArrayList<Database>();

		List<Database> dBList = glueUtil.getDatabases(glue, sourceGlueCatalogId);

		List<Map<String, AttributeValue>> itemList = new ArrayList<Map<String, AttributeValue>>();

		for(Database database : dBList) {

			System.out.println("\t" + database.getName());
		}
		for(Database database : dBList) {

			System.out.println("\t" + database.getName());

			Gson gson = new Gson();

			List<Table> dbTableList = glueUtil.getTables(glue, sourceGlueCatalogId, database.getName());
			for (Table table : dbTableList) {
				List<Partition> partitionList = glueUtil.getPartitions(glue, sourceGlueCatalogId, table.getDatabaseName(), table.getName());
				System.out.printf("\n\n\tDatabase: '%s', Table: %s, num_partitions: %d \n", table.getDatabaseName(), table.getName(), partitionList.size());
				TableWithPartitions tableWithParts = new TableWithPartitions();
				tableWithParts.setPartitionList(partitionList);
				tableWithParts.setTable(table);

				// Convert Table to JSON String
				String tableDDL = gson.toJson(table);


				Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
				item.put("table_id", new AttributeValue().withS(table.getName().concat("|").concat(table.getDatabaseName())));
				item.put("source_glue_catalog_id", new AttributeValue().withS(sourceGlueCatalogId));
				item.put("table_schema", new AttributeValue().withS(tableDDL)); 
				item.put("is_large_table", new AttributeValue().withS(Boolean.toString(false)));

				itemList.add(item);
				System.out.println("\t" +tableDDL);

			}
		}
		return itemList;
	}

	public int takeKeyInput(int max) {
		Integer keyentered= null;
		while(keyentered==null) {
			try {
				System.out.print("Please enter integer from 1 ,..., "+max+ ") :");
				@SuppressWarnings("resource")
				Scanner stdIn = new Scanner(System.in);
				keyentered = stdIn.nextInt();
			} catch (Exception e) {
				System.out.println("Invalid key entered, please retry");
			}
		}
		return keyentered;
	}

}
