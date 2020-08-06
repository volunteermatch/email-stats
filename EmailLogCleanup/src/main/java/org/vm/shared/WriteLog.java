package org.vm.shared;


import com.amazonaws.services.s3.AmazonS3;
import org.json.simple.JSONObject;

public interface WriteLog {
  
  void WriteLog(JSONObject jsonObject, String filename, AmazonS3 s3);
}
