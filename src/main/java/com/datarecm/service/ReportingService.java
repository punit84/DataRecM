package com.datarecm.service;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.ColumnInfo;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.Row;
import com.datarecm.service.config.ConfigService;

/**
 * @author Punit Jain
 *
 */
@Component
public class ReportingService {
	@Autowired
	private ConfigService config ;

	@Autowired
	public AthenaService athenaService;

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

	public void printRule2And3(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {
		int pass=0;
		int fail=0;
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.destination().getRules().size();
		createReportFile(sourcerulecount,destinationrulecount);

		for (int i = 0; i < sourcerulecount; i++) {
			Map<String, List<Object>> source  = sourceResutset.get(i);
			Map<String, List<Object>>  destination = destinationResutset.get(i);

			if (printRule(i, source, destination)) {
				pass++;
			}else {
				fail++;
			}

		}

		writeTextToFile("\n**********************Final Results***************************************************\n");
		writeTextToFile("Total Pass Rules : " +pass);
		writeTextToFile("\nTotal Failed Rules : " +fail);

	}
	public void printResult(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {
		int pass=0;
		int fail=0;
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.destination().getRules().size();
		createReportFile(sourcerulecount,destinationrulecount);

		for (int i = 0; i < sourcerulecount; i++) {
			Map<String, List<Object>> source  = sourceResutset.get(i);
			Map<String, List<Object>>  destination = destinationResutset.get(i);

			if (printRule(i, source, destination)) {
				pass++;
			}else {
				fail++;
			}

		}

		writeTextToFile("\n**********************Final Results***************************************************\n");
		writeTextToFile("Total Pass Rules : " +pass);
		writeTextToFile("\nTotal Failed Rules : " +fail);

	}

	// print rule and return true if strings are matching.
	public boolean compareRecData(int ruleIndex, Map<String, String> source, GetQueryResultsRequest getQueryResultsRequest) {
		writeTextToFile("\n**********************Evaluating RULE : "+ruleIndex+" *******************************************");
		writeTextToFile("\nSource record count : " +source.size());

		long time=System.currentTimeMillis();
		boolean isPass=false;
		int rowMatched=0;
		int rowCoundMatchedFailed=0;
		int counter=0;
		List<String> rowMatchingFailedRecords= new ArrayList<>();
		GetQueryResultsResult getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest);

		List<Row> results = getQueryResults.getResultSet().getRows();

		int name=0;
		int md5=1;

		//		Row fistRow=results.get(0);				// Process the row. The first row of the first page holds the column names.
		//		String columnName=fistRow.getData().get(name).getVarCharValue();
		//		String md5Column=fistRow.getData().get(md5).getVarCharValue();

		while (true) {
			results = getQueryResults.getResultSet().getRows();
			for (int i = 0; i < results.size(); i++) {
				if (counter ==0 && i==0) {
					continue;
				}
				Row row=results.get(i);				// Process the row. The first row of the first page holds the column names.

				String destID =row.getData().get(name).getVarCharValue();
				String destMD5=row.getData().get(md5).getVarCharValue();

				if (destMD5.equalsIgnoreCase(source.get(destID)) ) {
					source.remove(destID);
					rowMatched++;
					continue;
				}else {
					rowMatchingFailedRecords.add(destID);
					rowCoundMatchedFailed++;
				}

			}
			counter= counter + results.size() ;

			// If nextToken is null, there are no more pages to read. Break out of the loop.
			if (getQueryResults.getNextToken() == null) {
				break;
			}
			getQueryResults = athenaService.getAmazonAthenaClient().getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));

		}

		if (rowCoundMatchedFailed==0) {
			isPass = true;
		}

		writeTextToFile("\nTarget record count : " + --counter);

		writeTextToFile("\nRow matched  count : " +rowMatched);
		writeTextToFile("\nRow matched failed count : " +rowCoundMatchedFailed);
		if (rowCoundMatchedFailed>0) {
			writeTextToFile("\nRecords having failed match : " +rowMatchingFailedRecords);
		}
		if (source.keySet().size()>0) {
			writeTextToFile("\nMissing Record in Target : " +source.toString());
		}

		writeTextToFile("\n\nResults matching status : " +isPass);

		long timetaken = System.currentTimeMillis()-time;

		writeTextToFile("\nTime Taken in seconds : " +timetaken/1000);

		writeTextToFile("\n*************************************************************************\n");
		writeTextToFile("\nCurrent Date  : " +new Date());

		return isPass;

	}

	public boolean printRule(int ruleIndex, Map<String, List<Object>> source, Map<String, List<Object>> destination) {
		boolean isPass=false;

		writeTextToFile("\n**********************Evaluating RULE : "+ruleIndex+" *******************************************");
		printResultToFile("Source", source);
		printResultToFile("\nDestination", destination);
		String sourceString=source.toString();
		String destString=destination.toString();

		//destString=destString.replace("_col0", "count");
		//destString=destString.replace("_col1", "md5");

		if (sourceString.equals(destString)) {
			isPass=true;
		}

		writeTextToFile("\n\nResults matching status : " +isPass);

		writeTextToFile("\n*************************************************************************\n");

		return isPass;

	}

	public void createReportFile(int sourcerulecount,int destinationrulecount) throws IOException {
		file = new File(config.source().getReportFile());
		writeToFile("\t\t\t\tAWS - Data Reconciliation Module Report ", false);
		writeToFile("\n\t\t\t\t________________________________________\n\n", true);

		writeTextToFile("\nCurrent Date is :" +new Date());

		writeTextToFile("\nNo of Source rules : " +sourcerulecount);
		writeTextToFile("\nNo of Destination rules : " +destinationrulecount);
		writeTextToFile("\n");

	}

	public void printResultToFile(String type, int ruleno, Map<String, List<Object>>  resultset ) {
		try {

			String result= type + " : Execution result for rule:" + ruleno +" is :\n";

			writeToFile(resultset.toString(), true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printResultToFile(String type, Map<String, List<Object>> resultset ) {
		try {

			String result= "\n"+type + " execution result is :\n";
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

}