package com.datarecm.service;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.ColumnInfo;
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
	public boolean compareRecData(int ruleIndex, Map<String, List<Object>> source, GetQueryResultsResult getQueryResultsResult) {

		boolean isPass=false;

		List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();

		while (true) {
			List<Row> results = getQueryResultsResult.getResultSet().getRows();

			Row fistRow=results.get(0);				// Process the row. The first row of the first page holds the column names.
			int name=0;
			int md5=1;
			String columnName=fistRow.getData().get(name).getVarCharValue();
			String md5Column=fistRow.getData().get(md5).getVarCharValue();
			List<Object> serial= source.get(columnName);
			List<Object> md5Hash= source.get(md5Column);


			for (int i = 1; i < results.size(); i++) {
				Row row=results.get(i);				// Process the row. The first row of the first page holds the column names.

				String serialAthena=row.getData().get(name).getVarCharValue();
				String md5Athena=row.getData().get(md5).getVarCharValue();

				if (serialAthena.equalsIgnoreCase(serial.get(i-1).toString()) && md5Athena.equalsIgnoreCase(md5Hash.get(i-1).toString()) ) {

					continue;
				}else {
					System.out.println(serial.get(i-1).toString());
					System.out.println(serialAthena);
				}


			}


			//			for (Row row : results) {
			//				// Process the row. The first row of the first page holds the column names.
			//				processRow(row, columnInfoList);
			//			}
			// If nextToken is null, there are no more pages to read. Break out of the loop.
			if (getQueryResultsResult.getNextToken() == null) {
				break;
			}
			//getQueryResultsResult = athenaClient.getQueryResults(getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));

		}



		writeTextToFile("\n**********************Evaluating RULE : "+ruleIndex+" *******************************************");
		printResultToFile("Source", source);
		//printResultToFile("\nDestination", destination);
		//String sourceString=source.toString();
		//String destString=destination.toString();

		//destString=destString.replace("_col0", "count");
		//destString=destString.replace("_col1", "md5");



		writeTextToFile("\n\nResults matching status : " +isPass);

		writeTextToFile("\n*************************************************************************\n");

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