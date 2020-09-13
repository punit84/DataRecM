package com.datarecm.service;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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


	public void printResult(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {

		file = new File(config.source().getReportFile());
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.source().getRules().size();
		writeToFile("\t\t\t\tAWS - Data Reconciliation Module Report ", false);
		writeToFile("\n\t\t\t\t________________________________________\n\n", true);

		writeTextToFile("\nCurrent Date is :" +new Date());

		writeTextToFile("\nNo of Source rules : " +sourcerulecount);
		writeTextToFile("\nNo of Destination rules : " +destinationrulecount);
		writeTextToFile("\n");
		
		if (sourcerulecount!=destinationrulecount) {
			writeTextToFile("Rule count must be equal to run the report \n");	
			System.exit(0);
		}

		for (int i = 0; i < sourcerulecount; i++) {
			Map<String, List<Object>> source  = sourceResutset.get(i);
			Map<String, List<Object>>  destination = destinationResutset.get(i);
			writeTextToFile("\n**********************Evaluating RULE : "+i+" *******************************************");
			printResultToFile("Source", source);
			printResultToFile("\nDestination", destination);

			boolean isPass=false;
			String sourceString=source.toString();
			String destString=destination.toString();

			destString=destString.replace("_col0", "count");
			
			if (sourceString.equals(destString)) {
				isPass=true;
			}

			writeTextToFile("\n\nResults matching status : " +isPass);

			writeTextToFile("\n*************************************************************************\n");

		}
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