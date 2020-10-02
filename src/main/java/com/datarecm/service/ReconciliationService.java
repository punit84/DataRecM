package com.datarecm.service;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.datarecm.service.config.DBConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datarecm.service.config.AppConfig;

@Component
public class ReconciliationService {
	
	public static Log logger = LogFactory.getLog(ReconciliationService.class);
	@Autowired
	public S3AsyncOps s3Service;
	//public GlueService glueService;

	@Autowired
	public AthenaService athenaService;

	@Autowired
	SQLRunner sqlRunner ;

	@Autowired
	ReportingService report;

	int ruleIndexForMd5=4;
	int ruleIndexForUnmatchResult=6;
	int ruleIndexForMetadata=0;
	int ruleIndexForRecordCount=1;
	
	@Autowired
	private AppConfig appConfig;

	public File runRecTest(DBConfig sourceConfig, DBConfig targetConfig) throws Exception {
		sqlRunner.setSource(sourceConfig);
		athenaService.setTarget(targetConfig);
		report.setConfig(sourceConfig, targetConfig);
		long time=System.currentTimeMillis();
		logger.debug("************************");	
		int sourcerulecount=appConfig.getSourceRules().size();
		int destinationrulecount=appConfig.getTargetRules().size();
		if (sourcerulecount!=destinationrulecount) {
			logger.error("Rule count must be equal to run the report \n");	
			System.exit(0);
		}

		//Running all Athena Queries
		athenaService.submitAllQueriesAsync();

		File reportFile= report.createReportFile();
		runMetadataRules();

		if (sourceConfig.isEvaluateDataRules()) {
			//build other queries
			buildRuleAndRunAthenaQuery();

			runDataCount();
			//run count rule
			runDataComparisionRules(sourceConfig, targetConfig);
		}

		long timetaken = System.currentTimeMillis()-time;
		report.printEndOfReport(timetaken);
				
		return reportFile;
	}

	public String runRecTestURL(DBConfig sourceConfig, DBConfig targetConfig) throws Exception {
	
		File reportFile =runRecTest(sourceConfig, targetConfig);	
		String keyName = appConfig.getReportPath()+sourceConfig.getTableSchema()+"-"+sourceConfig.getTableName()+"_" +targetConfig.getDbname()+"-"+targetConfig.getTableName()+"-"+ (new Date()).toString();

		s3Service.uploadFile(appConfig.getS3bucket(),keyName , reportFile, targetConfig.getRegion());
		String url = s3Service.generateURL(appConfig.getS3bucket(), keyName);

		return url;
	}
	
	public void uploadToS3(DBConfig sourceConfig, DBConfig targetConfig, Map<String, String> sourceResult) throws Exception {
		
		String keyName = appConfig.getReportPath()+ AppConfig.MD5FILEPREFIX+ sourceConfig.getTableSchema()+".txt";

		s3Service.uploadText(appConfig.getS3bucket(), keyName , sourceResult, targetConfig.getRegion());
		logger.info("\n\nSource result uploaded to s3 " +   keyName);

	}
//	
//	public File runRecTest() throws Exception {
//		return runRecTest(defaultConfig.source(),defaultConfig.target());		
//	}

	private void runMetadataRules()
			throws Exception {
		List<String> rules = appConfig.getSourceRules();
		//Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

		logger.info("\n*******************Executing Source Query :"+ ruleIndexForMetadata+" *************");

		String updatedSourceRule=rules.get(ruleIndexForMetadata);

		Map<String, List<String>> sourceResult = sqlRunner.executeSQL(ruleIndexForMetadata , updatedSourceRule);
		//sqlResutset.put(ruleIndex, sourceResult);

		Map<String, List<String>> destResult   = athenaService.getProcessedQueriesResultSync(ruleIndexForMetadata);
		logger.info("\n*******************Execution successfull *************");
		report.buildSchemaQueries(sourceResult,destResult);
		report.printMetadataRules();
	}

	private void runDataCount()
			throws InterruptedException, IOException {
		//Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

		logger.info("\n*******************Executing Source Query :"+ ruleIndexForRecordCount+" *************");

		String updatedSourceRule=appConfig.getSourceRules().get(ruleIndexForRecordCount).trim();

		Map<String, List<String>> sourceResult = sqlRunner.executeSQL(ruleIndexForRecordCount , updatedSourceRule);
		//sqlResutset.put(ruleIndex, sourceResult);
		Map<String, List<String>> destResult   = athenaService.getProcessedQueriesResultSync(ruleIndexForRecordCount);
		int sourceCount = Integer.parseInt(sourceResult.get("count").get(0));
		int destCount =Integer.parseInt(destResult.get("count").get(0));


		logger.info("\n*******************Execution successfull *************");
		report.printCountRules(ruleIndexForRecordCount,sourceCount,destCount);
	}

	private void buildRuleAndRunAthenaQuery() throws InterruptedException {
		report.buildMD5Queries();
		athenaService.submitQuery(ruleIndexForMd5 ,report.destSchema.getQuery());		
	}
	
	private void runDataComparisionRules(DBConfig sourceConfig, DBConfig targetConfig) throws Exception {
		Map<String, String> sourceResult = sqlRunner.executeSQLForMd5(ruleIndexForMd5 , report.sourceSchema.getQuery());
		
		
		GetQueryResultsRequest getQueryResultsRequest = athenaService.getQueriesResultSync(ruleIndexForMd5);
		logger.info("Comparing using md5,rowcount : "+sourceResult.size() );
		
		CompletableFuture.runAsync(() -> {
			try {
				uploadToS3(sourceConfig, targetConfig, sourceResult);
				logger.info("Source file uploaded to s3... ");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		
		List<String> unmatchIDs = report.compareRecData(ruleIndexForMd5, sourceResult, getQueryResultsRequest);
		
		report.buildUnmatchedResultQueries(unmatchIDs);
		athenaService.submitQuery(ruleIndexForUnmatchResult ,report.destSchema.getFetchUnmatchRecordQuery());		

		Map<String, List<String>> sourceUnmatchResult = sqlRunner.executeSQL(ruleIndexForUnmatchResult, report.sourceSchema.getFetchUnmatchRecordQuery());

		
		
		Map<String, List<String>> destUnmatchedResults = athenaService.getProcessedQueriesResultSync(ruleIndexForUnmatchResult);

		report.printUnmatchResult(sourceUnmatchResult, destUnmatchedResults);

	}

}