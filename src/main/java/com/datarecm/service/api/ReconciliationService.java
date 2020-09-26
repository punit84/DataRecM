package com.datarecm.service.api;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import com.datarecm.service.DataRecMService;
import com.datarecm.service.config.ConfigProperties;
import com.datarecm.service.config.ConfigService;
import com.google.gson.Gson;

@Controller
public class ReconciliationService {

	@Autowired
	DataRecMService recmSrevice;
	@Autowired
	private ConfigService config ;


	@GetMapping("/report")
	@ResponseBody
	public ResponseEntity<Resource> getFile() throws Exception {

		File reportFile = recmSrevice.runRecTest();
		HttpHeaders header = new HttpHeaders();
		header.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=Report.text");
		header.add("Cache-Control", "no-cache, no-store, must-revalidate");
		header.add("Pragma", "no-cache");
		header.add("Expires", "0");

		Path path = Paths.get(reportFile.getAbsolutePath());
		ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(path));

		return ResponseEntity.ok()
				.headers(header)
				.contentLength(reportFile.length())
				.contentType(MediaType.TEXT_PLAIN)
				.body(resource);
	}

	@GetMapping("/source")
	@ResponseBody
	public String getSource() throws Exception {
		
		Gson gson = new Gson();
		return gson.toJson(config);
		
	}	 
	 
	@PostMapping("/validate")
	@ResponseBody
	public ConfigProperties doSomeThing(@RequestBody ConfigProperties prop){

		System.out.println(prop.toString());
		return prop;
	}

}