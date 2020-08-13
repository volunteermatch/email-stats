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

public class S3LogWriter implements WriteLog{
  
  
  /**
   * Method inherited from the WriteLog interface that will create a put request for a file
   * into an s3 bucket.
   * @param file that is added to s3 bucket.
   * @param bucketName is the name of the destination s3 bucket.
   */
  @Override
  public void writeLog(File file, String bucketName) {
    //Sets up the s3 bucket.
    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
  
    //Creates the PutObject request.
    try {
      InputStream stream = new FileInputStream(file);
      PutObjectRequest request
          = new PutObjectRequest(bucketName, file.getName(), stream, new ObjectMetadata());
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
