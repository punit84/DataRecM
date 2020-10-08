package com.datarecm.service.api;

import java.nio.file.Files;
import java.nio.file.Path;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.datarecm.service.ReconciliationService;
import com.datarecm.service.config.AppConfig;
import com.datarecm.service.config.ConfigTO;
import com.google.gson.Gson;

@RestController
public class ReconciliationServiceAPI {
/* 
{
  "source": {
    "region": "ap-south-1",
    "printUnmatchedRecordSize": 5,
    "hostname": "unicorngym.cyjiwrynkuap.ap-south-1.rds.amazonaws.com",
    "port": 5432,
    "username": "postgres",
    "password": "postgres123",
    "dbname": "unicorngym",
    "dbtype": "postgres",
    "reportFile": "./DRMRecReport",
    "primaryKey": "order_id",
    "url": "jdbc:mysql://",
    "timeout": 0,
    "evaluateDataRules": true,
    "tableName": "order",
    "tableSchema": "dms_sample",
    "ignoreList": [
      "timestamp"
    ]
  },
  "target": {
    "region": "ap-south-1",
    "printUnmatchedRecordSize": 0,
    "hostname": "unicorngym.cyjiwrynkuap.ap-south-1.rds.amazonaws.com",
    "port": 5432,
    "username": "postgres",
    "password": "postgres123",
    "dbname": "unicorngym",
    "dbtype": "csv",
    "primaryKey": "order_id",
    "output": "s3://query-result1234",
    "timeout": 100000,
    "evaluateDataRules": false,
    "tableName": "csvunicorn_gym_2020_csv",
    "tableSchema": "unicorngym",
    "ignoreList": [
      "timestamp"
    ]
  }
}
 * 
 * */
	@Autowired
	ReconciliationService recmSrevice;
	@Autowired
	private AppConfig config ;

	@PostMapping("/report")
	@ResponseBody
	public ResponseEntity<Resource> getFile(@Valid @RequestBody  ConfigTO prop) throws Exception {

		//prop.validate();
		Path reportFile = recmSrevice.runRecTest(prop.getSource(), prop.getTarget());
		HttpHeaders header = new HttpHeaders();
		header.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=Report.text");
		header.add("Cache-Control", "no-cache, no-store, must-revalidate");
		header.add("Pragma", "no-cache");
		header.add("Expires", "0");

		ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(reportFile));

		return ResponseEntity.ok()
				.headers(header)
				.contentLength(Files.size(reportFile))
				.contentType(MediaType.TEXT_PLAIN)
				.body(resource);
	}

	@PostMapping("/reportURL")
	@ResponseBody
	public String getFileInURL(@Valid @RequestBody  ConfigTO prop) throws Exception {

		//prop.validate();
		return recmSrevice.runRecTestURL(prop.getSource(), prop.getTarget());

	}

	@GetMapping("/config")
	@ResponseBody
	public String getSource() throws Exception {
		
		Gson gson = new Gson();
		return gson.toJson(config);
		
	}	 

}