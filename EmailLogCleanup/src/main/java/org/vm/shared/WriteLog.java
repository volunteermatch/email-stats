package org.vm.shared;


import java.io.File;

public interface WriteLog {
  
  void writeLog(File filename, String bucketName);
}
