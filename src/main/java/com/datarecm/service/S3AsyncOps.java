package com.datarecm.service;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Component;

import com.amazonaws.AmazonServiceException;
import com.datarecm.service.util.PresignedUrlUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.appmesh.model.HttpMethod;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@Component
public class S3AsyncOps {

	public static S3AsyncClient s3Client=null;
	public void uploadFile(String bucketName, String keyName,File file, String regionStr) {

		S3AsyncClient s3Client = getS3Client(regionStr);

		PutObjectRequest objectRequest = PutObjectRequest.builder()
				.bucket(bucketName)
				.key(keyName)
				.build();

		// Put the object into the bucket
		CompletableFuture<PutObjectResponse> future = s3Client.putObject(objectRequest,
				AsyncRequestBody.fromFile(file)
				);
		future.whenComplete((resp, err) -> {
			onComplete(s3Client, resp, err);
		});

		future.join();
	}

	public void uploadText(String bucketName, String keyName,Map<String, String> sourceResult,String regionStr) throws JsonProcessingException {

		Gson gson= new Gson();

		S3AsyncClient s3Client = getS3Client(regionStr);

		PutObjectRequest objectRequest = PutObjectRequest.builder()
				.bucket(bucketName)
				.key(keyName)
				.build();

		s3Client.putObject(objectRequest, AsyncRequestBody.fromString( sourceResult.toString()));
	}



	public void uploadText(String bucketName, String keyName,String text, String regionStr) {

		S3AsyncClient s3Client = getS3Client(regionStr);

		PutObjectRequest objectRequest = PutObjectRequest.builder()
				.bucket(bucketName)
				.key(keyName)
				.build();

		// Put the object into the bucket
		CompletableFuture<PutObjectResponse> future = s3Client.putObject(objectRequest,
				AsyncRequestBody.fromString(text)
				);
		future.whenComplete((resp, err) -> {
			onComplete(s3Client, resp, err);
		});

	}
	public S3AsyncClient getS3Client(String regionStr) {

		if (s3Client==null) {
			Region region = Region.of(regionStr);
			s3Client= S3AsyncClient.builder()
					.region(region)
					.build();
		}

		return s3Client;
	}

	public String generateURL(String bucketName, String keyName) {
		String url = null;


		try {

			// Generate the presigned URL.
			System.out.println("Generating pre-signed URL.");
			url = PresignedUrlUtil.getURLString(bucketName, keyName);

			System.out.println("Pre-Signed URL: " + url.toString());

		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process 
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		return url;

	}
	public void onComplete(S3AsyncClient client, PutObjectResponse resp, Throwable err) {
		try {
			if (resp != null) {
				System.out.println("Object uploaded. Details: " + resp);
			} else {
				// Handle error
				err.printStackTrace();
			}
		} finally {
			// Lets the application shut down. Only close the client when you are completely done with it.
			//client.close();
		}
	}
}

