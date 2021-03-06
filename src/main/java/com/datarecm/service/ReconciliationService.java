package com.datarecm.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.util.CollectionUtils;
import com.datarecm.service.athena.AthenaService;
import com.datarecm.service.config.AppConfig;
import com.datarecm.service.config.AppConstants;
import com.datarecm.service.config.DBConfig;
import com.datarecm.service.report.ReportFileUtil;
import com.datarecm.service.report.ReportingService;
import com.datarecm.service.report.S3AsyncOps;
import com.datarecm.service.source.SQLRunner;

@Component
public class ReconciliationService {

	public static Log logger = LogFactory.getLog(ReconciliationService.class);
	@Autowired
	public S3AsyncOps s3Service;
	//public GlueService glueService;

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

	
	public Path runRecTest(DBConfig sourceConfig, DBConfig targetConfig) throws Exception {
		ReportFileUtil fileUtil=null;
		long time=System.currentTimeMillis();

		try {

			String fileName = appConfig.getReportFile()+"-"+sourceConfig.getDbtype()+"-"+targetConfig.getDbtype()+".txt";
			fileUtil= new ReportFileUtil(fileName);
			AthenaService athenaService = new AthenaService();
			athenaService.setTarget(targetConfig);
			athenaService.setAppConfig(appConfig);
			report.setConfig(sourceConfig, targetConfig);
			logger.debug("************************");	
			int sourcerulecount=appConfig.getSourceRules().size();
			int destinationrulecount=appConfig.getTargetRules().size();
			if (sourcerulecount!=destinationrulecount) {
				logger.error("Rule count must be equal to run the report \n");	
				System.exit(0);
			}

			//Running all Athena Queries
			athenaService.submitAllQueriesAsync();

			fileUtil.createReportFile(sourceConfig, targetConfig);;
			runMetadataRules(fileUtil,athenaService,sourceConfig);

			if (sourceConfig.isEvaluateDataRules()) {
				//build other queries
				buildRuleAndRunAthenaQuery(fileUtil,athenaService);

				runDataCount(fileUtil,athenaService,sourceConfig);
				//run count rule
				runDataComparisionRules(sourceConfig, targetConfig,fileUtil,athenaService);
			}

			long timetaken = System.currentTimeMillis()-time;
			fileUtil.printEndOfReport(timetaken);

		} catch (Exception e) {
			long timetaken = System.currentTimeMillis()-time;
			fileUtil.printError(e, timetaken);
			//throw e;
		}finally {
			File finalReport= fileUtil.getFilePath().toFile();
			LocalDate today = LocalDate.now();
			String folder= today.getYear()+"/"+today.getMonth()+"/"+today.getDayOfMonth()+"/";

			String keyName = appConfig.getReportPath()+folder+sourceConfig.getTableSchema()+"-"+sourceConfig.getTableName()+"_" +targetConfig.getDbname()+"-"+targetConfig.getTableName()+"-"+ (new Date()).toString();
			CompletableFuture.runAsync(() -> {
				try {
					logger.info("Source file uploading to s3... ");
					s3Service.uploadFile(appConfig.getS3bucket(),keyName , finalReport, targetConfig.getRegion());
					logger.info("Source file uploaded successfully to s3... ");
				} catch (Exception e) {
					logger.error("S3 upload failed for report file: "+keyName);
					logger.error(e.getLocalizedMessage());
				}
			});

		}

		return fileUtil.getFilePath();

	}

	public String runRecTestURL(DBConfig sourceConfig, DBConfig targetConfig) throws Exception {

		Path reportFile =runRecTest(sourceConfig, targetConfig);	
		LocalDate today = LocalDate.now();

		String folder= today.getYear()+"/"+today.getMonth()+"/"+today.getDayOfMonth()+"/";

		String keyName = appConfig.getReportPath()+folder+sourceConfig.getTableSchema()+"-"+sourceConfig.getTableName()+"_" +targetConfig.getDbname()+"-"+targetConfig.getTableName()+"-"+ (new Date()).toString();

		s3Service.uploadFile(appConfig.getS3bucket(),keyName , reportFile.toFile(), targetConfig.getRegion());
		String url = s3Service.generateURL(appConfig.getS3bucket(), keyName);

		return url;
	}

	public void uploadToS3(DBConfig sourceConfig, DBConfig targetConfig, String sourceResult) throws Exception {

		LocalDate today = LocalDate.now();

		String folder= today.getYear()+"/"+today.getMonth()+"/"+today.getDayOfMonth()+"/";

		String keyName = appConfig.getReportPath()+folder+ AppConstants.MD5FILEPREFIX+ sourceConfig.getTableSchema()+"-"+sourceConfig.getTableName()+".txt";

		s3Service.uploadText(appConfig.getS3bucket(), keyName , sourceResult, targetConfig.getRegion());
		logger.info("\n\nSource result uploaded to s3 " +   keyName);

	}
	//	
	//	public File runRecTest() throws Exception {
	//		return runRecTest(defaultConfig.source(),defaultConfig.target());		
	//	}

	private void runMetadataRules(ReportFileUtil fileUtil,AthenaService athenaService, DBConfig sourceConfig)
			throws Exception {
		List<String> rules = appConfig.getSourceRules();
		//Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

		logger.info("\n*******************Executing Source Query :"+ ruleIndexForMetadata+" *************");

		String updatedSourceRule=rules.get(ruleIndexForMetadata);

		Map<String, List<String>> sourceResult = sqlRunner.executeSQL(ruleIndexForMetadata , updatedSourceRule,sourceConfig);
		//sqlResutset.put(ruleIndex, sourceResult);

		Map<String, List<String>> destResult   = athenaService.getProcessedQueriesResultSync(ruleIndexForMetadata);
		logger.info("\n*******************Execution successfull *************");
		fileUtil.buildSchemaQueries(sourceResult,destResult);
		report.printMetadataRules(fileUtil);
	}

	private void runDataCount(ReportFileUtil fileUtil,AthenaService athenaService, DBConfig sourceConfig)
			throws InterruptedException, IOException, ClassNotFoundException, SQLException {
		//Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

		logger.info("\n*******************Executing Source Query :"+ ruleIndexForRecordCount+" *************");

		String updatedSourceRule=appConfig.getSourceRules().get(ruleIndexForRecordCount).trim();

		Map<String, List<String>> sourceResult = sqlRunner.executeSQL(ruleIndexForRecordCount , updatedSourceRule,sourceConfig);
		//sqlResutset.put(ruleIndex, sourceResult);
		Map<String, List<String>> destResult   = athenaService.getProcessedQueriesResultSync(ruleIndexForRecordCount);
		int sourceCount = Integer.parseInt(sourceResult.get("count").get(0));
		int destCount =Integer.parseInt(destResult.get("count").get(0));


		logger.info("\n*******************Execution successfull *************");
		report.printCountRules(ruleIndexForRecordCount,sourceCount,destCount, fileUtil);
	}

	private void buildRuleAndRunAthenaQuery(ReportFileUtil fileUtil,AthenaService athenaService) throws Exception {
		report.buildMD5Queries(fileUtil);
		athenaService.submitQuery(ruleIndexForMd5 ,fileUtil.destSchema.getQuery());		
	}

	private void runDataComparisionRules(DBConfig sourceConfig, DBConfig targetConfig, ReportFileUtil fileUtil,AthenaService athenaService) throws Exception {
		Map<String, String> sourceResult = sqlRunner.executeSQLForMd5(ruleIndexForMd5 , fileUtil.sourceSchema.getQuery(),sourceConfig);

		GetQueryResultsRequest getQueryResultsRequest = athenaService.getQueriesResultSync(ruleIndexForMd5);
		logger.info("Comparing using md5,rowcount : "+sourceResult.size() );

		logger.info("Source file uploading to s3... ");

		uploadToS3(sourceConfig, targetConfig, sourceResult.toString());
		logger.info("Source file uploaded successfully to s3... ");

		Map<String, String> sourceMD5MapCopy = new ConcurrentHashMap<>(sourceResult);

		List<String> unmatchIDs = report.compareRecData(ruleIndexForMd5, sourceMD5MapCopy, getQueryResultsRequest, fileUtil,athenaService);
		logger.info("Unmatched ids" + unmatchIDs);
		if (CollectionUtils.isNullOrEmpty(unmatchIDs)) {
			return;
		}
		report.buildUnmatchedResultQueries(unmatchIDs,fileUtil);
		athenaService.submitQuery(ruleIndexForUnmatchResult ,fileUtil.destSchema.getFetchUnmatchRecordQuery());		

		Map<String, List<String>> sourceUnmatchResult = sqlRunner.executeSQL(ruleIndexForUnmatchResult, fileUtil.sourceSchema.getFetchUnmatchRecordQuery(),sourceConfig);

		Map<String, List<String>> destUnmatchedResults = athenaService.getProcessedQueriesResultSync(ruleIndexForUnmatchResult);

		report.printUnmatchResult(sourceUnmatchResult, destUnmatchedResults, fileUtil);

	}

	private void runDataComparisionRulesSet(DBConfig sourceConfig, DBConfig targetConfig, ReportFileUtil fileUtil,AthenaService athenaService) throws Exception {
		Set<String> sourceResultSet = sqlRunner.executeSQLForMd5Set(ruleIndexForMd5 , fileUtil.sourceSchema.getQuery(),sourceConfig);


		GetQueryResultsRequest getQueryResultsRequest = athenaService.getQueriesResultSync(ruleIndexForMd5);
		logger.info("Comparing using md5,rowcount : "+sourceResultSet.size() );


		List<String> unmatchIDs = report.compareRecDataSet(ruleIndexForMd5, sourceResultSet, getQueryResultsRequest, fileUtil,athenaService);
		logger.info("Unmatched ids" + unmatchIDs);
		if (CollectionUtils.isNullOrEmpty(unmatchIDs)) {
			return;
		}
		report.buildUnmatchedResultQueries(unmatchIDs,fileUtil);
		athenaService.submitQuery(ruleIndexForUnmatchResult ,fileUtil.destSchema.getFetchUnmatchRecordQuery());		

		Map<String, List<String>> sourceUnmatchResult = sqlRunner.executeSQL(ruleIndexForUnmatchResult, fileUtil.sourceSchema.getFetchUnmatchRecordQuery(),sourceConfig);



		Map<String, List<String>> destUnmatchedResults = athenaService.getProcessedQueriesResultSync(ruleIndexForUnmatchResult);

		report.printUnmatchResult(sourceUnmatchResult, destUnmatchedResults, fileUtil);

	}


}