package com.datarecm.service;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.datarecm.service.config.DBConfig;
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
			runDataComparisionRules();
		}

		long timetaken = System.currentTimeMillis()-time;
		report.printEndOfReport(timetaken);
		String key = appConfig.getReportPath()+sourceConfig.getTableSchema()+"-"+sourceConfig.getTableName()+"_" +targetConfig.getDbname()+"-"+targetConfig.getTableName()+"-"+ (new Date()).toString();
		
		
		s3Service.uploadFile(appConfig.getS3bucket(),key , reportFile, targetConfig.getRegion());
		
		return reportFile;
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
	
	private void runDataComparisionRules() throws InterruptedException {
		Map<String, String> sourceResult = sqlRunner.executeSQLForMd5(ruleIndexForMd5 , report.sourceSchema.getQuery());
		GetQueryResultsRequest getQueryResultsRequest = athenaService.getQueriesResultSync(ruleIndexForMd5);
		logger.info("Comparing using md5,rowcount : "+sourceResult.size() );
		List<String> unmatchIDs = report.compareRecData(ruleIndexForMd5, sourceResult, getQueryResultsRequest);
		
		
		report.buildUnmatchedResultQueries(unmatchIDs);
		athenaService.submitQuery(ruleIndexForUnmatchResult ,report.destSchema.getFetchUnmatchRecordQuery());		

		Map<String, List<String>> sourceUnmatchResult = sqlRunner.executeSQL(ruleIndexForUnmatchResult, report.sourceSchema.getFetchUnmatchRecordQuery());

		
		
		Map<String, List<String>> destUnmatchedResults = athenaService.getProcessedQueriesResultSync(ruleIndexForUnmatchResult);

		report.printUnmatchResult(sourceUnmatchResult, destUnmatchedResults);

	}

}