package com.datarecm.service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.util.CollectionUtils;
import com.datarecm.service.AppConstants.TargetType;
import com.datarecm.service.config.AppConfig;
import com.datarecm.service.config.DBConfig;

/**
 * Service to create a report
 * @author Punit Jain
 *
 */

@Component
public class ReportingService {
	//@Autowired
	//private ConfigService defaultConfig ;

	@Autowired
	private AppConfig appConfig;

	DBConfig sourceConfig;
	DBConfig targetConfig;
	//private int MAX_UNMATCH_COUNT;

	//@Autowired
	//private AthenaService athenaService;

	@Autowired
	private QueryBuilder queryBuilder;

	public static Log logger = LogFactory.getLog(ReportingService.class);

	//File file = null;

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


	public void buildMD5Queries(ReportFileUtil fileUtil) throws Exception {

		fileUtil.sourceSchema.setPrimaryKey(sourceConfig.getPrimaryKey());
		fileUtil.destSchema.setPrimaryKey(targetConfig.getPrimaryKey());

		TargetType targeTtype= TargetType.UNKNOWN;

		if (AppConstants.FILE_TYPE_CSV.equalsIgnoreCase(targetConfig.getDbtype()) ) {
			targeTtype= TargetType.CSV;
		}else if(AppConstants.FILE_TYPE_PARQUET.equalsIgnoreCase(targetConfig.getDbtype())){
			targeTtype= TargetType.PARQUET;
		}else {

			throw new Exception("Not supported Target db type : "+targetConfig.getDbtype());

		}

		queryBuilder.createFetchDataQueries(fileUtil.sourceSchema, fileUtil.destSchema , sourceConfig.getIgnoreList(),targeTtype);
		logger.info("Source Query is :" +fileUtil.sourceSchema.getQuery());
		logger.info("Dest Query is :" +fileUtil.destSchema.getQuery());
	}

	public void buildUnmatchedResultQueries(List<String> unmatchIDs, ReportFileUtil fileUtil) {

		queryBuilder.createFetchUnmatchedDataQueries(fileUtil.sourceSchema, fileUtil.destSchema, unmatchIDs );
		logger.info(fileUtil.sourceSchema.getFetchUnmatchRecordQuery());
		logger.info(fileUtil.destSchema.getFetchUnmatchRecordQuery());
	}



	public void printMetadataRules(ReportFileUtil fileUtil) throws Exception {
		int ruleindex=0;
		boolean match=true;
		//String status = AppConstants.MATCH;

		//source.ruledesc[1]=Rule 2: Matching of Field order
		//		source.ruledesc[2]=Rule 3: Matching of Field Name
		//		source.ruledesc[3]=Rule 4: Matching of Field Data Type

		//rule 1 count
		int sourceFieldCount=fileUtil.sourceSchema.getColumnNameList().size();
		int destFieldCount=fileUtil.destSchema.getColumnNameList().size();

		fileUtil.printRuleHeader(appConfig.getRuleDesc().get(ruleindex));

		fileUtil.printMatchStatus((sourceFieldCount == destFieldCount));

		fileUtil.printEndSummary(("\nSource Field count : "+ sourceFieldCount), ("\nTarget Field count : "+ destFieldCount));

		ruleindex++;
		match = runFieldNameComparision(ruleindex, match,fileUtil);
		ruleindex++;

		//Rule 3
		fileUtil.printRuleHeader(appConfig.getRuleDesc().get(ruleindex));

		fileUtil.printMatchStatus(match);
		fileUtil.printEndSummaryOrderFields();

		ruleindex++;

		//Rule 4
		runFieldTypeComparision(ruleindex, match,fileUtil);
	}

	public void runFieldTypeComparision(int ruleindex, boolean match, ReportFileUtil fileUtil) {
		fileUtil.printRuleHeader(appConfig.getRuleDesc().get(ruleindex));
		List<Integer> compatibleFields = new ArrayList<>();
		List<Integer> matched = new ArrayList<>();
		List<Integer> unMatched = new ArrayList<>();

		fieldComarator(compatibleFields, matched, unMatched, fileUtil.sourceSchema, fileUtil.destSchema);
		fileUtil.printFieldTypeComparision(compatibleFields, matched, unMatched, fileUtil.sourceSchema, fileUtil.destSchema);;
	}


	public void fieldComarator(List<Integer> compatibleFields, List<Integer> matched, List<Integer> unMatched, TableInfo sourceSchema, TableInfo destSchema) {
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
	}

	public boolean runFieldNameComparision(int ruleindex, boolean match,ReportFileUtil fileUtil) throws Exception {
		TableInfo sourceSchema = fileUtil.sourceSchema;
		TableInfo destSchema = fileUtil.destSchema;

		///////2  name
		fileUtil.printRuleHeader(appConfig.getRuleDesc().get(ruleindex));
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
		fileUtil.printMatchStatus(match);
		fileUtil.printEndSummary("\nSource Field names : "+ sourceSchema.getColumnNameList().toString(), "\nTarget Field names : "+ destSchema.getColumnNameList().toString());

		return match;
	}

	//	private void printResult(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {
	//		int pass=0;
	//		int fail=0;
	//		int sourcerulecount=appConfig.getSourceRules().size();
	//		int destinationrulecount=appConfig.getTargetRules().size();
	//		createReportFile();
	//
	//		for (int i = 0; i < sourcerulecount; i++) {
	//			Map<String, List<Object>> source  = sourceResutset.get(i);
	//			Map<String, List<Object>>  destination = destinationResutset.get(i);
	//
	//			if (printRule(i, source, destination)) {
	//				pass++;
	//			}else {
	//				fail++;
	//			}
	//
	//		}
	//
	//		fileUtil.writeTextToFile("\n********************** Final Results ***************************************************\n");
	//		fileUtil.writeTextToFile("Total MATCH Rules : " +pass);
	//		fileUtil.writeTextToFile("\nTotal MISMATCH Rules : " +fail);
	//
	//	}

	// print rule and return true if strings are matching.
	public List<String> compareRecData(int ruleIndex, Map<String, String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest, ReportFileUtil fileUtil,AthenaService athenaService) {
		logger.info("Starting: Comapring records using MD5");
		List<String> ignoreList = 	sourceConfig.getIgnoreList();
		//String ruleDescCount=appConfig.getRuleDesc().get(ruleIndex);
		String ruleDescValue=appConfig.getRuleDesc().get(ruleIndex+1);
		String primaryKey = sourceConfig.getPrimaryKey();
		int sourceCount=sourceMD5Map.size();
		List<String> unmatchedIDs=new ArrayList<String>();
		//int targetCount= compareCount(sourceMD5Map, getQueryResultsRequest);

		fileUtil.writeTextToFile("\n**********************************************************************************\n");
		fileUtil.writeTextToFile(ruleDescValue);
		if (!CollectionUtils.isNullOrEmpty(ignoreList)) {
			fileUtil.writeTextToFile("\nSkipping Columns : "+ignoreList.toString());
		}
		fileUtil.writeTextToFile("\n**********************************************************************************\n");
		logger.info("Starting MD5 Comparision .. ");
		compareValueUsingMD5(sourceMD5Map, getQueryResultsRequest.clone(), athenaService) ; //first row reserved for column name
		logger.info(" MD5 Comparision finished .. ");

		if (sourceMD5Map.size()==0) {

			fileUtil.writeTextToFile("Result = " + AppConstants.MATCH);
			//fileUtil.writeTextToFile("\nCount of records compared : " +countRecordCompared);
			fileUtil.writeTextToFile("\nCount of matching records : " +sourceCount);

		}else {
			fileUtil.writeTextToFile("Result = " + AppConstants.MISMATCH);
			//fileUtil.writeTextToFile("\nCount of records compared : " +countRecordCompared);
			fileUtil.writeTextToFile("\nCount of matching records : " +(sourceCount- sourceMD5Map.size()));
			fileUtil.writeTextToFile("\nCount of non-matching records : " +sourceMD5Map.size());
			fileUtil.writeTextToFile("\n-------------------------------------------------");

			fileUtil.writeTextToFile("\nMax Mismatch Record Print count set as : " +sourceConfig.getPrintUnmatchedRecordSize()+"\n");
			fileUtil.writeTextToFile("\nPrimary Keys of non-matching records : " +primaryKey);
			unmatchedIDs = getMaxUnmatchedIDs(sourceMD5Map);
			fileUtil.writeTextToFile(unmatchedIDs.toString());

			fileUtil.writeTextToFile("\n-------------------------------------------------");

		}
		//fileUtil.writeTextToFile("\n**********************************************************************************\n");

		//fileUtil.writeTextToFile("\n**********************************************************************************\n");
		//fileUtil.writeTextToFile("\nCurrent Date  : " +new Date());
		logger.info("Completed: Comapring records using MD5");

		return unmatchedIDs;
	}

	public List<String> compareRecDataSet(int ruleIndex, Set<String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest, ReportFileUtil fileUtil,AthenaService athenaService) {
		List<String> ignoreList = 	sourceConfig.getIgnoreList();
		//String ruleDescCount=appConfig.getRuleDesc().get(ruleIndex);
		String ruleDescValue=appConfig.getRuleDesc().get(ruleIndex+1);
		String primaryKey = sourceConfig.getPrimaryKey();
		int sourceCount=sourceMD5Map.size();
		List<String> unmatchedIDs=new ArrayList<String>();
		//int targetCount= compareCount(sourceMD5Map, getQueryResultsRequest);

		fileUtil.writeTextToFile("\n**********************************************************************************\n");
		fileUtil.writeTextToFile(ruleDescValue);
		if (!CollectionUtils.isNullOrEmpty(ignoreList)) {
			fileUtil.writeTextToFile("\nSkipping Columns : "+ignoreList.toString());
		}
		fileUtil.writeTextToFile("\n**********************************************************************************\n");
		logger.info("Starting MD5 Comparision .. ");
		compareValueUsingMD5Set(sourceMD5Map, getQueryResultsRequest.clone(), athenaService) ; //first row reserved for column name
		logger.info(" MD5 Comparision finished .. ");

		if (sourceMD5Map.size()==0) {

			fileUtil.writeTextToFile("Result = " + AppConstants.MATCH);
			//fileUtil.writeTextToFile("\nCount of records compared : " +countRecordCompared);
			fileUtil.writeTextToFile("\nCount of matching records : " +sourceCount);

		}else {
			fileUtil.writeTextToFile("Result = " + AppConstants.MISMATCH);
			//fileUtil.writeTextToFile("\nCount of records compared : " +countRecordCompared);
			fileUtil.writeTextToFile("\nCount of matching records : " +(sourceCount- sourceMD5Map.size()));
			fileUtil.writeTextToFile("\nCount of non-matching records : " +sourceMD5Map.size());
			fileUtil.writeTextToFile("\n-------------------------------------------------");

			fileUtil.writeTextToFile("\nMax Mismatch Record Print count set as : " +sourceConfig.getPrintUnmatchedRecordSize()+"\n");
			fileUtil.writeTextToFile("\nPrimary Keys of non-matching records : " +primaryKey);
			unmatchedIDs = getMaxUnmatchedIDsSet(sourceMD5Map);
			fileUtil.writeTextToFile(unmatchedIDs.toString());

			fileUtil.writeTextToFile("\n-------------------------------------------------");

		}
		//fileUtil.writeTextToFile("\n**********************************************************************************\n");

		//fileUtil.writeTextToFile("\n**********************************************************************************\n");
		//fileUtil.writeTextToFile("\nCurrent Date  : " +new Date());

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

	private List<String> getMaxUnmatchedIDsSet(Set< String> sourceMD5Map) {
		List<String> unmatchIDs =  new ArrayList<String>();
		int max =	sourceConfig.getPrintUnmatchedRecordSize();

		for (String recordId : sourceMD5Map) {
			if (max<=0) {
				break;
			}

			String id= recordId.substring(0, recordId.indexOf("-"));
			logger.info("unmatch id " + id);
			unmatchIDs.add(id);
			max--;
		}

		return unmatchIDs;
	}
	private void printUnmatchedRecords(Map<String, String> sourceMD5Map, ReportFileUtil fileUtil) {
		int max =	sourceConfig.getPrintUnmatchedRecordSize();

		for (String recordId : sourceMD5Map.keySet()) {
			if (max<=0) {
				break;
			}
			fileUtil.writeTextToFile("\n" +recordId);
			max--;
		}
	}

	public void printCountRules(int ruleIndex, int sourceCount, int targetCount, ReportFileUtil fileUtil ) throws IOException {

		fileUtil.printRuleHeader(appConfig.getRuleDesc().get(ruleIndex+3));
		fileUtil.printMatchStatus(sourceCount == targetCount);
		fileUtil.printEndSummary("\nSource record count : " +sourceCount, "\nTarget record count : " + targetCount);

	}


	public void printUnmatchResult(Map<String, List<String>> sourceUnmatchResult, Map<String, List<String>> destUnmatchedResults, ReportFileUtil fileUtil) {
		fileUtil.writeTextToFile("\nPrinting Columns having mismatch");

		for (String columnKey : sourceUnmatchResult.keySet()) {

			String sourceColumn=sourceUnmatchResult.get(columnKey).toString();
			String destColumn=destUnmatchedResults.get(columnKey).toString();
			if (!(sourceColumn.equals(destColumn))) {
				fileUtil.writeTextToFile("\nSource Column("+columnKey+") with MISMATCH : " +sourceColumn);
				fileUtil.writeTextToFile("\nTarget Column("+columnKey+") with MISMATCH : " +destColumn +"\n");
			}
		}
		fileUtil.writeTextToFile("\n**********************************************************************************\n");

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


	/*
	public boolean printRule(int ruleIndex, Map<String, List<Object>> source, Map<String, List<Object>> destination) {
		fileUtil.writeTextToFile("\n**********************************************************************************\n");
		fileUtil.writeTextToFile(appConfig.getRuleDesc().get(ruleIndex));
		fileUtil.writeTextToFile("\n**********************************************************************************\n");

		String sourceString=source.toString();
		String destString=destination.toString();
		if (sourceString.equals(destString)) {
			fileUtil.writeTextToFile("Result = " + AppConstants.MATCH);
			printResultToFile("\nSource", source);
			printResultToFile("\nTarget", destination);

			fileUtil.writeTextToFile("\n**********************************************************************************\n");
			return true;

		}else {
			fileUtil.writeTextToFile("Result = " + AppConstants.MISMATCH);
			printResultToFile("Source", source);
			printResultToFile("Target", destination);
			fileUtil.writeTextToFile("\n**********************************************************************************\n");
			return false;

		}

	}


	 */
	public void printToFile(String type,Map<Integer, Map<String, List<Object>>> athenaResutset, ReportFileUtil fileUtil ) throws IOException {
		for (int i = 0; i < athenaResutset.keySet().size(); i++) {
			Map<String, List<Object>> resultset= athenaResutset.get(i);
			fileUtil.printResultToFile(type,i, resultset);

		}
	}

	public void setConfig(DBConfig sourceConfig,DBConfig targetConfig) {
		this.sourceConfig=sourceConfig;
		this.targetConfig= targetConfig;
	}
	public void compareValueUsingMD5(Map<String, String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest, AthenaService athenaService) {


		//Map<String, String> unmatchedMD5Map= new HashMap<String, String>();
		GetQueryResultsResult getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest);

		//List<CompletableFuture> futures = new ArrayList();
		int i= 0;
		while (true) {
			getQueryResults.getResultSet().getRows().parallelStream().forEach(row-> {
				compareMD5Section(sourceMD5Map,row );

			});
			//List<Row> results = new ArrayList<Row>();
			//results.addAll(getQueryResults.getResultSet().getRows());

			//	futures.add(CompletableFuture.runAsync(() -> compareMD5Section(sourceMD5Map,results )));
			//logger.info(i++);
			//counter= counter + results.size() ;

			// If nextToken is null, there are no more pages to read. Break out of the loop.
			if (getQueryResults.getNextToken() == null) {
				break;
			}
			getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));

		}
		//CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
		// .thenRun(() -> logger.info("Ended doing things"+futures.size()));

	}

	public void compareValueUsingMD5Set(Set<String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest, AthenaService athenaService) {


		GetQueryResultsResult getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest);
		int i= 0;
		while (true) {
			List<Row> results = new ArrayList<Row>();
			results.addAll(getQueryResults.getResultSet().getRows());

			compareMD5SectionSet(sourceMD5Map,results );
			if (getQueryResults.getNextToken() == null) {
				break;
			}
			getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));

		}

	}
	public void compareMD5Section(Map<String, String> sourceMD5Map, List<Row> results) {
		results.parallelStream().forEach(row-> {
			String destID =row.getData().get(0).getVarCharValue();
			String destMD5=row.getData().get(1).getVarCharValue();
			if (destMD5.equalsIgnoreCase(sourceMD5Map.get(destID)) ) {
				//logger.info(destID);
				sourceMD5Map.remove(destID);
			}else {
				logger.debug("Mismatch found on record " +destID );
			}

		});
	}
	public void compareMD5Section(Map<String, String> sourceMD5Map, Row row) {
		String destID =row.getData().get(0).getVarCharValue();
		String destMD5=row.getData().get(1).getVarCharValue();
		if (destMD5.equalsIgnoreCase(sourceMD5Map.get(destID)) ) {
			//logger.info(destID);
			sourceMD5Map.remove(destID);
		}else {
			logger.debug("Mismatch found on record " +destID );
		}

	}
	public void compareMD5SectionSet(Set<String> sourceMD5Map, List<Row> results) {

		for (int i = 0; i < results.size(); i++) {
			//logger.info(i);
			Row row=results.get(i);				// Process the row. The first row of the first page holds the column names.
			String destID =row.getData().get(0).getVarCharValue();
			String destMD5=row.getData().get(1).getVarCharValue();
			sourceMD5Map.remove(destID+"-"+destMD5);
		}
	}


}