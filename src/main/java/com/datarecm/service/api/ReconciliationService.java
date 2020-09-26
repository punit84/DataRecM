package com.datarecm.service.api;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.datarecm.service.DataRecMService;

@Controller
public class ReconciliationService {

	@Autowired
	DataRecMService recmSrevice;

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

}