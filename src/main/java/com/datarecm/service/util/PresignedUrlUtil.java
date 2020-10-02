package com.datarecm.service.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.time.Duration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.utils.IoUtils;

public class PresignedUrlUtil {

 public static void main(String[] args) {
   
      if (args.length < 2) {
        System.out.println("Please specify a bucket name and a key name that represents a text file");
        System.exit(1);
     }

     String bucketName = args[0];
     String keyName = args[1];

     // Create an S3Presigner by using the default AWS Region and credentials
     S3Presigner presigner = S3Presigner.create();
     getPresignedUrl(presigner, bucketName,keyName);

 }
   // snippet-start:[presigned.java2.getobjectpresigned.main]
    public static void getPresignedUrl( S3Presigner presigner, String bucketName,String keyName ) {

     try {

         PresignedGetObjectRequest presignedGetObjectRequest = getURL(presigner, bucketName, keyName);

         // Create a JDK HttpURLConnection for communicating with S3
         HttpURLConnection connection = (HttpURLConnection) presignedGetObjectRequest.url().openConnection();

         // Specify any headers that the service needs (not needed when isBrowserExecutable is true)
         presignedGetObjectRequest.httpRequest().headers().forEach((header, values) -> {
             values.forEach(value -> {
                 connection.addRequestProperty(header, value);
             });
         });

         // Send any request payload that the service needs (not needed when isBrowserExecutable is true)
         if (presignedGetObjectRequest.signedPayload().isPresent()) {
             connection.setDoOutput(true);
             try (InputStream signedPayload = presignedGetObjectRequest.signedPayload().get().asInputStream();
                  OutputStream httpOutputStream = connection.getOutputStream()) {
                 IoUtils.copy(signedPayload, httpOutputStream);
             }
         }

         // Download the result of executing the request
         try (InputStream content = connection.getInputStream()) {
             System.out.println("Service returned response: ");
             IoUtils.copy(content, System.out);
         }

         /*
          *  It's recommended that you close the S3Presigner when it is done being used, because some credential
          * providers (e.g. if your AWS profile is configured to assume an STS role) require system resources
          * that need to be freed. If you are using one S3Presigner per application (as recommended), this
          * usually isn't needed
          */
         presigner.close();

     } catch (S3Exception e) {
         e.getStackTrace();
     } catch (IOException e) {
         e.getStackTrace();
     }
     // snippet-end:[presigned.java2.getobjectpresigned.main]
 }
	public static PresignedGetObjectRequest getURL(S3Presigner presigner, String bucketName, String keyName) {
		// Create a GetObjectRequest to be pre-signed
         GetObjectRequest getObjectRequest =
                 GetObjectRequest.builder()
                         .bucket(bucketName)
                         .key(keyName)
                         .build();

         // Create a GetObjectPresignRequest to specify the signature duration
         GetObjectPresignRequest getObjectPresignRequest =
                 GetObjectPresignRequest.builder()
                         .signatureDuration(Duration.ofMinutes(10))
                         .getObjectRequest(getObjectRequest)
                         .build();

         // Generate the presigned request
         PresignedGetObjectRequest presignedGetObjectRequest =
                 presigner.presignGetObject(getObjectPresignRequest);

         // Log the presigned URL
         System.out.println("Presigned URL: " + presignedGetObjectRequest.url());
		return presignedGetObjectRequest;
	}
	
	public static String getURLString(String bucketName, String keyName) {
		
	     S3Presigner presigner = S3Presigner.create();

		// Create a GetObjectRequest to be pre-signed
         GetObjectRequest getObjectRequest =
                 GetObjectRequest.builder()
                         .bucket(bucketName)
                         .key(keyName)
                         .build();

         // Create a GetObjectPresignRequest to specify the signature duration
         GetObjectPresignRequest getObjectPresignRequest =
                 GetObjectPresignRequest.builder()
                         .signatureDuration(Duration.ofMinutes(100))
                         .getObjectRequest(getObjectRequest)
                         .build();

         // Generate the presigned request
         PresignedGetObjectRequest presignedGetObjectRequest =
                 presigner.presignGetObject(getObjectPresignRequest);

         // Log the presigned URL
         System.out.println("Presigned URL: " + presignedGetObjectRequest.url());
		return presignedGetObjectRequest.url().toString();
	}
}