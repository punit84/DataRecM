package com.datarecm.service;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.util.CollectionUtils;
import com.datarecm.service.config.DBConfig;
import com.datarecm.service.config.AppConfig;

/**
 * Service to create a report
 * @author Punit Jain
 *
 */
@Component
public class ReportingService {
	//@Autowired
	//private ConfigService defaultConfig ;
	TableInfo sourceSchema;
	TableInfo destSchema;

	@Autowired
	private AppConfig appConfig;

	DBConfig sourceConfig;
	DBConfig targetConfig;
	private int MAX_UNMATCH_COUNT;

	@Autowired
	private AthenaService athenaService;

	@Autowired
	private QueryBuilder queryBuilder;

	public static Log logger = LogFactory.getLog(ReportingService.class);

	File file = null;

	//	public static void writeToFileBufferedWriter(String msg) {
	//		FileWriter fileWriter;
	//		BufferedWriter bufferedWriter;
	//		try {
	//			fileWriter = new FileWriter(file.getAbsoluteFile(), true); // true to append
	//			bufferedWriter = new BufferedWriter(fileWriter);
	//			bufferedWriter.write(msg);
	//			bufferedWriter.close();
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}
	//	}

	public  void writeTextToFile(String msg) {
		try {
			// 3rd parameter boolean append = true
			writeToFile(msg, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void buildSchemaQueries(Map<String, List<String>> sourceResult , Map<String, List<String>> destResult ) {

		sourceSchema = new TableInfo(sourceResult);
		destSchema = new TableInfo(destResult);
	}

	public void buildMD5Queries() {

		sourceSchema.setPrimaryKey(sourceConfig.getPrimaryKey());
		destSchema.setPrimaryKey(targetConfig.getPrimaryKey());
		queryBuilder.createFetchDataQueries(sourceSchema, destSchema , sourceConfig.getIgnoreList());
		logger.info("Source Query is :" +sourceSchema.getQuery());
		logger.info("Dest Query is :" +destSchema.getQuery());
	}

	public void buildUnmatchedResultQueries(List<String> unmatchIDs) {

		queryBuilder.createFetchUnmatchedDataQueries(sourceSchema, destSchema, unmatchIDs);
		logger.info(sourceSchema.getFetchUnmatchRecordQuery());
		logger.info(destSchema.getFetchUnmatchRecordQuery());
	}



	public void printMetadataRules() throws Exception {
		int ruleindex=0;
		boolean match=true;
		//String status = AppConstants.MATCH;

		//source.ruledesc[1]=Rule 2: Matching of Field order
		//		source.ruledesc[2]=Rule 3: Matching of Field Name
		//		source.ruledesc[3]=Rule 4: Matching of Field Data Type

		//rule 1 count
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(appConfig.getRuleDesc().get(ruleindex));
		writeTextToFile("\n**********************************************************************************\n");

		int sourceFieldCount=sourceSchema.getColumnNameList().size();
		int destFieldCount=destSchema.getColumnNameList().size();

		if (sourceFieldCount == destFieldCount) {
			writeTextToFile("Result = " + AppConstants.MATCH);
		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}
		writeTextToFile("\nSource Field count : "+ sourceFieldCount);
		writeTextToFile("\nTarget Field count : "+ destFieldCount);
		writeTextToFile("\n**********************************************************************************\n");
		ruleindex++;

		match = runFieldNameComparision(ruleindex, match);
		ruleindex++;

		//Rule 3
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(appConfig.getRuleDesc().get(ruleindex));
		writeTextToFile("\n**********************************************************************************\n");

		if (match) {
			writeTextToFile("Result = " + AppConstants.MATCH);
		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}
		writeTextToFile("\nOrder of source fields : "+ sourceSchema.getNameWithSequence().toString());
		writeTextToFile("\nOrder of target fields : "+ destSchema.getNameWithSequence().toString());
		writeTextToFile("\n**********************************************************************************\n");
		ruleindex++;

		//Rule 4
		runFieldTypeComparision(ruleindex, match);
	}

	public void runFieldTypeComparision(int ruleindex, boolean match) {
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(appConfig.getRuleDesc().get(ruleindex));
		writeTextToFile("\n**********************************************************************************\n");
		List<Integer> compatibleFields = new ArrayList<>();
		List<Integer> matched = new ArrayList<>();
		List<Integer> unMatched = new ArrayList<>();

		for (int i = 0; i < sourceSchema.fieldCount; i++) {
			String sourceType=sourceSchema.getColumnTypeList().get(i).toLowerCase();
			String destType=destSchema.getColumnTypeList().get(i).toLowerCase();

			if (sourceType.equals(destType)) {
				matched.add(i);
			}else if (AppConstants.FILE_TYPE_CSV.equalsIgnoreCase(targetConfig.getDbtype())) {

				switch (sourceType) {
				case "character":
					if ("varchar".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				case "varchar":
				case "bpchar(1)":
				case "text":
				case "time":
				case "timestamp":
				case "date":
					if ("string".equalsIgnoreCase(destType) || "varchar".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				case "float4":
				case "float8":
					if ("string".equalsIgnoreCase(destType) || "varchar".equalsIgnoreCase(destType) || "double".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;


				case "bool":
					if ("Boolean".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;
				case "integer":

				case "int2":
				case "smallint":
				case "int4":
				case "int8":
					if ("Bigint".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				case "numeric":
				case "numeric(12,2)":
				case "money":

					if ("Double".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}

					break;

				default:
					unMatched.add(i);
					break;
				}
			}else if (AppConstants.FILE_TYPE_PARQUET.equalsIgnoreCase(targetConfig.getDbtype())) {

				//				UNMATCHED  - Source Field(Type) : [order_status(character),order_value(numeric)]
				//						UNMATCHED  - Target Field(Type) : [order_status(varchar),order_value(decimal(12,2))]

				switch (sourceType) {
				case "character":
					if ("varchar".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				case "varchar":
				case "bpchar(1)":
				case "text":
				case "time":
				case "timestamp":
				case "date":
					if ("string".equalsIgnoreCase(destType) || "varchar".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				case "float4":
				case "float8":
					if ("string".equalsIgnoreCase(destType) || "varchar".equalsIgnoreCase(destType) || "double".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;


				case "bool":
					if ("Boolean".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;
				case "integer":

				case "int2":
				case "smallint":
				case "int4":
				case "int8":
					if ("Bigint".equalsIgnoreCase(destType)) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				case "money":
				case "numeric":
				case "numeric(12,2)":
					if ("Double".equalsIgnoreCase(destType) || destType.contains("decimal")) {
						compatibleFields.add(i);
					}else {
						unMatched.add(i);
					}
					break;

				default:
					unMatched.add(i);
					break;
				}

			}
		}
		if (unMatched.size()==0 ) {
			if (compatibleFields.size()==0) {
				writeTextToFile("Result = " + AppConstants.MATCH);

			}else {
				writeTextToFile("Result = " + AppConstants.COMPATIBLE);

			}

		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}
		writeTextToFile("\nMATCHED("+matched.size()+")    - Source Field(Type) : "+ sourceSchema.getNameWithType(matched).toString());
		writeTextToFile("\nMATCHED("+matched.size()+")    - Target Field(Type) : "+ destSchema.getNameWithType(matched).toString());		

		writeTextToFile("\nCOMPATIBLE("+compatibleFields.size()+") - Source Field(Type) : "+ sourceSchema.getNameWithType(compatibleFields).toString());
		writeTextToFile("\nCOMPATIBLE("+compatibleFields.size()+") - Target Field(Type) : "+ destSchema.getNameWithType(compatibleFields).toString());		
		writeTextToFile("\nUNMATCHED("+unMatched.size()+")  - Source Field(Type) : "+ sourceSchema.getNameWithType(unMatched).toString());
		writeTextToFile("\nUNMATCHED("+unMatched.size()+")  - Target Field(Type) : "+ destSchema.getNameWithType(unMatched).toString());		

		writeTextToFile("\n**********************************************************************************\n");

	}

	public boolean runFieldNameComparision(int ruleindex, boolean match) throws Exception {
		///////2  name
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(appConfig.getRuleDesc().get(ruleindex));
		writeTextToFile("\n**********************************************************************************\n");
		if (sourceSchema.fieldCount != destSchema.fieldCount) {

			String errormsg= "Schema fields are not equal source "+ sourceSchema.fieldCount+" dest column fileds"+destSchema.fieldCount ;
			logger.error(errormsg);
			throw new Exception(errormsg);
			
		}
		for (int i = 0; i < sourceSchema.fieldCount; i++) {

			if (!(sourceSchema.getColumnNameList().get(i).toString().equalsIgnoreCase(destSchema.getColumnNameList().get(i).toString()))) {
				logger.info(sourceSchema.getColumnNameList().get(i).toString());
				match= false;
				break;
			}


			match = true;
		}

		if (match) {
			writeTextToFile("Result = " + AppConstants.MATCH);
		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}
		writeTextToFile("\nSource Field names : "+ sourceSchema.getColumnNameList().toString());
		writeTextToFile("\nTarget Field names : "+ destSchema.getColumnNameList().toString());

		writeTextToFile("\n**********************************************************************************\n");
		return match;
	}

	private void printResult(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {
		int pass=0;
		int fail=0;
		int sourcerulecount=appConfig.getSourceRules().size();
		int destinationrulecount=appConfig.getTargetRules().size();
		createReportFile();

		for (int i = 0; i < sourcerulecount; i++) {
			Map<String, List<Object>> source  = sourceResutset.get(i);
			Map<String, List<Object>>  destination = destinationResutset.get(i);

			if (printRule(i, source, destination)) {
				pass++;
			}else {
				fail++;
			}

		}

		writeTextToFile("\n********************** Final Results ***************************************************\n");
		writeTextToFile("Total MATCH Rules : " +pass);
		writeTextToFile("\nTotal MISMATCH Rules : " +fail);

	}

	// print rule and return true if strings are matching.
	public List<String> compareRecData(int ruleIndex, Map<String, String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest) {
		List<String> ignoreList = 	sourceConfig.getIgnoreList();
		//String ruleDescCount=appConfig.getRuleDesc().get(ruleIndex);
		String ruleDescValue=appConfig.getRuleDesc().get(ruleIndex+1);
		String primaryKey = sourceConfig.getPrimaryKey();
		int sourceCount=sourceMD5Map.size();
		List<String> unmatchedIDs=null;
		//int targetCount= compareCount(sourceMD5Map, getQueryResultsRequest);

		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(ruleDescValue);
		if (!CollectionUtils.isNullOrEmpty(ignoreList)) {
			writeTextToFile("\nSkipping Columns : "+ignoreList.toString());
		}
		writeTextToFile("\n**********************************************************************************\n");
		int countRecordCompared = compareValueUsingMD5(sourceMD5Map, getQueryResultsRequest.clone()) -1; //first row reserved for column name

		if (sourceMD5Map.size()==0) {

			writeTextToFile("Result = " + AppConstants.MATCH);
			writeTextToFile("\nCount of records compared : " +countRecordCompared);
			writeTextToFile("\nCount of matching records : " +sourceCount);

		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
			writeTextToFile("\nCount of records compared : " +countRecordCompared);
			writeTextToFile("\nCount of matching records : " +(sourceCount- sourceMD5Map.size()));
			writeTextToFile("\nCount of non-matching records : " +sourceMD5Map.size());
			writeTextToFile("\n-------------------------------------------------");

			writeTextToFile("\nMax Mismatch Record Print count set as : " +sourceConfig.getPrintUnmatchedRecordSize()+"\n");
			writeTextToFile("\nPrimary Keys of non-matching records : " +primaryKey);
			unmatchedIDs = getMaxUnmatchedIDs(sourceMD5Map);
			writeTextToFile(unmatchedIDs.toString());

			writeTextToFile("\n-------------------------------------------------");

		}
		//writeTextToFile("\n**********************************************************************************\n");

		//writeTextToFile("\n**********************************************************************************\n");
		//writeTextToFile("\nCurrent Date  : " +new Date());

		return unmatchedIDs;
	}

	private List<String> getMaxUnmatchedIDs(Map<String, String> sourceMD5Map) {
		List<String> unmatchIDs =  new ArrayList<String>();
		int max =	sourceConfig.getPrintUnmatchedRecordSize();

		for (String recordId : sourceMD5Map.keySet()) {
			if (max<=0) {
				break;
			}
			unmatchIDs.add(recordId);
			max--;
		}

		return unmatchIDs;
	}

	private void printUnmatchedRecords(Map<String, String> sourceMD5Map) {
		int max =	sourceConfig.getPrintUnmatchedRecordSize();

		for (String recordId : sourceMD5Map.keySet()) {
			if (max<=0) {
				break;
			}
			writeTextToFile("\n" +recordId);
			max--;
		}
	}

	public void printEndOfReport(long timetaken) {
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile("Time Taken in seconds : " +timetaken/1000);
		writeTextToFile("\nEnd of the report!!");
		writeTextToFile("\n**********************************************************************************\n");
	}

	public void printCountRules(int ruleIndex, int sourceCount, int targetCount ) throws IOException {
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(appConfig.getRuleDesc().get(ruleIndex+3));
		writeTextToFile("\n**********************************************************************************\n");

		if (sourceCount == targetCount) {
			writeTextToFile("Result = " + AppConstants.MATCH);
		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}

		writeTextToFile("\nSource record count : " +sourceCount);
		writeTextToFile("\nTarget record count : " + targetCount);
		writeTextToFile("\n**********************************************************************************\n");


	}


	public void printUnmatchResult(Map<String, List<String>> sourceUnmatchResult, Map<String, List<String>> destUnmatchedResults) {
		writeTextToFile("\nPrinting Columns having mismatch");

		for (String columnKey : sourceUnmatchResult.keySet()) {

			String sourceColumn=sourceUnmatchResult.get(columnKey).toString();
			String destColumn=destUnmatchedResults.get(columnKey).toString();
			if (!(sourceColumn.equals(destColumn))) {
				writeTextToFile("\nSource Column("+columnKey+") with MISMATCH : " +sourceColumn);
				writeTextToFile("\nTarget Column("+columnKey+") with MISMATCH : " +destColumn +"\n");
			}
		}
		writeTextToFile("\n**********************************************************************************\n");

	}
	/*
	public int compareCount(Map<String, String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest) {
		//	logger.info("Calulating Target Record count is " +recordCount);

		int recordCount = 0;
		GetQueryResultsResult getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest);
		List<Row> results = null;

		while (true) {
			results = getQueryResults.getResultSet().getRows();
			recordCount=recordCount+results.size();

			//results = getQueryResults.getResultSet().getRows();
			// If nextToken is null, there are no more pages to read. Break out of the loop.
			//getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));			

			logger.debug("Target Record count is " +recordCount);
			if (getQueryResults.getNextToken() == null) {
				break;
			}
			getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));
		}

		return recordCount-1;
	}*/

	public int compareValueUsingMD5(Map<String, String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest) {
		int nameKey=0;
		int md5Key=1;
		int recordCompareCount=0;

		Map<String, String> unmatchedMD5Map= new HashMap<String, String>();
		GetQueryResultsResult getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest);

		List<Row> results = null;

		while (true) {
			int unmatchCount=0;
			results = getQueryResults.getResultSet().getRows();
			for (int i = 0; i < results.size(); i++) {
				if (recordCompareCount ==0 && i==0) {
					continue;
				}
				Row row=results.get(i);				// Process the row. The first row of the first page holds the column names.
				String destID =row.getData().get(nameKey).getVarCharValue();
				String destMD5=row.getData().get(md5Key).getVarCharValue();

				recordCompareCount++;
				if (destMD5.equalsIgnoreCase(sourceMD5Map.get(destID)) ) {
					sourceMD5Map.remove(destID);
					continue;
				}else {
					logger.debug("Mismatch found on record " +destID );
					//rowMatchingFailedRecords.add(destID);
					unmatchCount++;
					//					unmatchedMD5Map.put( destID,sourceMD5Map.get(destID));
					//					if (unmatchCount>=config.source().getPrintUnmatchedRecordSize()) {
					//						sourceMD5Map = unmatchedMD5Map;
					//						return recordCompareCount;
					//					}
				}

			}
			//counter= counter + results.size() ;

			// If nextToken is null, there are no more pages to read. Break out of the loop.
			if (getQueryResults.getNextToken() == null) {
				break;
			}
			getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));

		}
		return recordCompareCount;
	}

	public boolean printRule(int ruleIndex, Map<String, List<Object>> source, Map<String, List<Object>> destination) {
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(appConfig.getRuleDesc().get(ruleIndex));
		writeTextToFile("\n**********************************************************************************\n");

		String sourceString=source.toString();
		String destString=destination.toString();
		if (sourceString.equals(destString)) {
			writeTextToFile("Result = " + AppConstants.MATCH);
			printResultToFile("\nSource", source);
			printResultToFile("\nTarget", destination);

			writeTextToFile("\n**********************************************************************************\n");
			return true;

		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
			printResultToFile("Source", source);
			printResultToFile("Target", destination);
			writeTextToFile("\n**********************************************************************************\n");
			return false;

		}

	}

	File createReportFile() throws IOException {
		String fileName = appConfig.getReportFile()+"-"+sourceConfig.getDbtype()+"-"+targetConfig.getDbtype()+".txt";
		file = new File(fileName);
		writeToFile("\t\t\t\tAWS - Data Reconciliation Module Report ", false);
		writeToFile("\n\t\t\t\t________________________________________\n\n", true);

		writeTextToFile("\nCurrent Date is :" +new Date());
		writeTextToFile("\nSource Type :'" +sourceConfig.getDbtype().toUpperCase()+"'");
		writeTextToFile("\nTarget Type :'" +targetConfig.getDbtype().toUpperCase()+"'");

		writeTextToFile("\nNo of Metadata rules : " +4);

		if (sourceConfig.isEvaluateDataRules()) {
			writeTextToFile("\nNo of Data validation rules : " +2);
		}
		writeTextToFile("\n");
		return file;

	}

	public void printResultToFile(String type, int ruleno, Map<String, List<Object>>  resultset ) {
		try {

			//String result= type + " : Execution result for rule:" + ruleno +" is :\n";

			writeToFile(resultset.toString(), true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printResultToFile(String type, Map<String, List<Object>> resultset ) {
		try {

			String result= "\n"+type + " execution result is :";
			writeToFile(result.toString(), true);
			writeToFile(resultset.toString(), true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void printToFile(String type,Map<Integer, Map<String, List<Object>>> athenaResutset ) throws IOException {
		for (int i = 0; i < athenaResutset.keySet().size(); i++) {
			Map<String, List<Object>> resultset= athenaResutset.get(i);

			printResultToFile(type,i, resultset);

		}
	}

	private void writeToFile(String msg, boolean append) throws IOException {
		FileUtils.writeStringToFile(file, msg, append);
	}

	public TableInfo getSourceSchema() {
		return sourceSchema;
	}

	public void setSourceSchema(TableInfo sourceSchema) {
		this.sourceSchema = sourceSchema;
	}

	public TableInfo getDestSchema() {
		return destSchema;
	}

	public void setDestSchema(TableInfo destSchema) {
		this.destSchema = destSchema;
	}

	public void setConfig(DBConfig sourceConfig,DBConfig targetConfig) {
		this.sourceConfig=sourceConfig;
		this.targetConfig= targetConfig;
	}



}