package com.datarecm.service;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class ReportingService {

	static File file = new File("./report.txt");
	static String fileContent = "Execution result of source  \n";
 
	public static void main(String[] args) {
 
		writeToFileApacheCommonIO(fileContent);
 
		System.out.println("File Updated.");
	}
 
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
 
	public static void writeToFileApacheCommonIO(String msg) {
		try {
			// 3rd parameter boolean append = true
			FileUtils.writeStringToFile(file, msg, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}