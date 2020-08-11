package org.vm.shared;


import com.amazonaws.services.s3.AmazonS3;
import java.io.File;

public interface WriteLog {
  
  void WriteLog(File filename, String bucketName);
}
