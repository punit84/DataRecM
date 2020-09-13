package com.datarecm.service;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.ConfigService;

@Component
public class ReportingService {
	@Autowired
	private ConfigService config ;

	static File file = new File("./report.txt");


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
			FileUtils.writeStringToFile(file, msg, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public void printResult(Map<Integer,Map<String, List<Object>>> sourceResutset, Map<Integer,Map<String, List<Object>>> destinationResutset ) throws IOException {
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.source().getRules().size();
		FileUtils.writeStringToFile(file, "Data Reconsilation Module Report \n", false);

		//writeTextToFile("Data Reconsilation Module Report \n");
		writeTextToFile("\nNo of Source rules :" +sourcerulecount);
		writeTextToFile("\nNo of Destination rules :" +destinationrulecount);
		if (sourcerulecount!=destinationrulecount) {
			writeTextToFile("Rule count must be equal to run the report \n");	
			System.exit(0);
		}

		for (int i = 0; i < sourcerulecount; i++) {
			Map<String, List<Object>> source  = sourceResutset.get(i);
			Map<String, List<Object>>  destination = destinationResutset.get(i);
			writeTextToFile("\n********************** RULE : "+i+" *******************************************");
			printResultToFile("Source", source);
			printResultToFile("Destination", destination);

			boolean isPass=false;
			if (source.equals(destination)) {
				isPass=true;
			}

			writeTextToFile("\nResults matching status " +isPass);

			writeTextToFile("\n*************************************************************************\n");

		}
	}

	public void printResultToFile(String type, int ruleno, Map<String, List<Object>>  resultset ) {
		try {

			String result= type + " : Execution result for rule:" + ruleno +" is :\n";

			FileUtils.writeStringToFile(file, resultset.toString(), true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printResultToFile(String type, Map<String, List<Object>> resultset ) {
		try {

			String result= "\n"+type + " execution result is :\n";

			FileUtils.writeStringToFile(file, result.toString(), true);

			FileUtils.writeStringToFile(file, resultset.toString(), true);

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

}