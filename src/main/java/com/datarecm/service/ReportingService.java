package com.datarecm.service;
import java.io.File;
import java.io.IOException;
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
import com.datarecm.service.config.ConfigService;

/**
 * Service to create a report
 * @author Punit Jain
 *
 */
@Component
public class ReportingService {
	@Autowired
	private ConfigService config ;
	TableInfo sourceSchema;
	TableInfo destSchema;
	
	private int MAX_UNMATCH_COUNT;

	@Autowired
	public AthenaService athenaService;
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

		sourceSchema.setPrimaryKey(config.source().getPrimaryKey());
		destSchema.setPrimaryKey(config.destination().getPrimaryKey());
		QueryBuilder.createFetchDataQueries(sourceSchema, destSchema , config.source().getIgnoreList());
		logger.info("Source Query is :" +sourceSchema.getQuery());
		logger.info("Dest Query is :" +destSchema.getQuery());
	}

	public void printMetadataRules() throws IOException {
		int ruleindex=0;
		boolean match=true;
		//String status = AppConstants.MATCH;

		//source.ruledesc[1]=Rule 2: Matching of Field order
		//		source.ruledesc[2]=Rule 3: Matching of Field Name
		//		source.ruledesc[3]=Rule 4: Matching of Field Data Type

		//rule 1 count
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(config.source().getRuledesc().get(ruleindex));
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


		///////2  name
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(config.source().getRuledesc().get(ruleindex));
		writeTextToFile("\n**********************************************************************************\n");

		for (int i = 0; i < sourceSchema.fieldCount; i++) {
			if (!(sourceSchema.getColumnNameList().get(i).toString().equalsIgnoreCase(destSchema.getColumnNameList().get(i).toString()))) {
				System.out.println(sourceSchema.getColumnNameList().get(i).toString());
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
		ruleindex++;

		//Rule 3
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(config.source().getRuledesc().get(ruleindex));
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
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(config.source().getRuledesc().get(ruleindex));
		writeTextToFile("\n**********************************************************************************\n");
		for (int i = 0; i < sourceSchema.fieldCount; i++) {
			if (!(sourceSchema.getColumnTypeList().get(i).equals(destSchema.getColumnTypeList().get(i)))) {
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
		writeTextToFile("\nSource Field and Data Type : "+ sourceSchema.getNameWithType().toString());
		writeTextToFile("\nTarget Field and Data Type : "+ destSchema.getNameWithType().toString());		
		writeTextToFile("\n**********************************************************************************\n");
	}

	private void printResult(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {
		int pass=0;
		int fail=0;
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.destination().getRules().size();
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
	public void compareRecData(int ruleIndex, Map<String, String> sourceMD5Map, GetQueryResultsRequest getQueryResultsRequest) {
		List<String> ignoreList = 	config.source().getIgnoreList();
		String ruleDescCount=config.source().getRuledesc().get(ruleIndex);
		String ruleDescValue=config.source().getRuledesc().get(ruleIndex+1);
		String primaryKey = config.source().getPrimaryKey();
		int sourceCount=sourceMD5Map.size();

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
			writeTextToFile("\nSource Primary Keys of non-matching records : " +primaryKey);
			writeTextToFile("\n-------------------------------------------------");

			writeTextToFile("\nMax Mismatch Record Print count set as : " +config.source().getPrintUnmatchedRecordSize()+"\n");

			printUnmatchedRecords(sourceMD5Map);


			writeTextToFile("\n-------------------------------------------------");

		}
		writeTextToFile("\n**********************************************************************************\n");

		//writeTextToFile("\n**********************************************************************************\n");
		//writeTextToFile("\nCurrent Date  : " +new Date());
	}

	public void printUnmatchedRecords(Map<String, String> sourceMD5Map) {
		int max=	config.source().getPrintUnmatchedRecordSize();

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
		writeTextToFile(config.source().getRuledesc().get(ruleIndex+3));
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
		writeTextToFile(config.source().getRuledesc().get(ruleIndex));
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

	void createReportFile() throws IOException {
		file = new File(config.source().getReportFile());
		writeToFile("\t\t\t\tAWS - Data Reconciliation Module Report ", false);
		writeToFile("\n\t\t\t\t________________________________________\n\n", true);

		writeTextToFile("\nCurrent Date is :" +new Date());
		writeTextToFile("\nSource Type :'" +config.source().getDbtype().toUpperCase()+"'");
		writeTextToFile("\nTarget Type :'" +config.destination().getDbtype().toUpperCase()+"'");

		writeTextToFile("\nNo of Metadata rules : " +4);

		if (config.source().isEvaluateDataRules()) {
			writeTextToFile("\nNo of Data validation rules : " +2);
		}
		writeTextToFile("\n");

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


}