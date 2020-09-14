package org.vm.shared;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;

/**
 * The S3LogWriter class takes is a class that implements the WriteLog interface and specifiying it
 * to write to a file to an s3 bucket.
 */
public class S3LogWriter implements WriteLog{
  
  private final String bucket;
  
  /**
   * The constructor takes in the name of the destination S3 bucket.
   * @param bucketName is the name of the destination bucket in AWS.
   */
  public S3LogWriter(String bucketName) {
    bucket = bucketName;
  }
  
  /**
   * Method inherited from the WriteLog interface that will create a put request for a file
   * into an s3 bucket.
   */
  @Override
  public void writeLog(File file, String filename) throws IOException {
    //Sets up the s3 bucket.
    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
  
    //Creates the PutObject request.
    try {
      PutObjectRequest request
          = new PutObjectRequest(bucket, filename, file);
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentType("text/csv");
      request.setMetadata(metadata);
      s3Client.putObject(request);
    
    } catch (SdkClientException e) {
      e.printStackTrace();
    }
  }
}
