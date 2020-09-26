package com.datarecm.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.datarecm.service.config.ConfigService;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain
 *
 */

@SpringBootApplication
public class DataRecMApplication {
	public static Log logger = LogFactory.getLog(DataRecMApplication.class);


	@Autowired
	public GlueService glueService;

	@Autowired
	public AthenaService athenaService;

	@Autowired
	SQLRunner sqlRunner ;

	@Autowired
	private ConfigService config ;

	@Autowired
	ReportingService report;
	@Autowired
	private QueryBuilder queryBuilder;
	int ruleIndexForMd5=4;
	int ruleIndexForUnmatchResult=6;
	int ruleIndexForMetadata=0;
	int ruleIndexForRecordCount=1;

	@PostConstruct
	public void runRecTest() throws Exception {
		long time=System.currentTimeMillis();
		logger.debug("************************");	
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.destination().getRules().size();
		if (sourcerulecount!=destinationrulecount) {
			logger.error("Rule count must be equal to run the report \n");	
			System.exit(0);
		}

		//Running all Athena Queries
		athenaService.submitAllQueriesAsync();

		report.createReportFile();
		runMetadataRules();

		if (config.source().isEvaluateDataRules()) {
			//build other queries
			buildRuleAndRunAthenaQuery();

			runDataCount();
			//run count rule
			runDataComparisionRules();
		}

		long timetaken = System.currentTimeMillis()-time;
		report.printEndOfReport(timetaken);

	}

	public void runMetadataRules()
			throws InterruptedException, IOException {
		List<String> rules = config.source().getRules();
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

	public void runDataCount()
			throws InterruptedException, IOException {
		//Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

		logger.info("\n*******************Executing Source Query :"+ ruleIndexForRecordCount+" *************");

		String updatedSourceRule=config.source().getRules().get(ruleIndexForRecordCount).trim();

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
	public void runDataComparisionRules() throws InterruptedException {
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


	public static void main(String[] args) {
		SpringApplication.run(DataRecMApplication.class, args);	
		System.exit(0);
	}




}
