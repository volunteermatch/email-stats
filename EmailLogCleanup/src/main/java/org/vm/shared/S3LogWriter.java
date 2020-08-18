package org.vm.shared;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * The S3LogWriter class takes is a class that implements the WriteLog interface and specifiying it
 * to write to a file to an s3 bucket.
 */
public class S3LogWriter implements WriteLog{
  
  private final String bucket;
  
  /**
   * The constructor takes in the file to be written and the name of the destination S3 bucket.
   * @param bucketName is the name of the destination bucket in AWS.
   */
  public S3LogWriter(String bucketName) {
    bucket = bucketName;
    
    //Change to use write log to take in file, not in constructor
    
  }
  
  /**
   * Method inherited from the WriteLog interface that will create a put request for a file
   * into an s3 bucket.
   */
  @Override
  public void writeLog(File file) {
    //Sets up the s3 bucket.
    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
  
    //Creates the PutObject request.
    try {
      InputStream stream = new FileInputStream(file);
      PutObjectRequest request
          = new PutObjectRequest(bucket, file.getName(), stream, new ObjectMetadata());
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentType("plain/text");
      metadata.addUserMetadata("title", "someTitle");
      request.setMetadata(metadata);
      s3Client.putObject(request);
    
    } catch (SdkClientException | FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
