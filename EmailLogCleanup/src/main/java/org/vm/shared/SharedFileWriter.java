package org.vm.shared;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public interface SharedFileWriter {
  
  File createFile(String filePath);
  
  FileWriter createFileWriter(String filePath);
  
  void writeToFile(Object o);
  
  void deleteFile();
  
  void flushAndClose();
  
  File getFile();
}
