package org.vm.shared;

import java.io.File;
import java.io.IOException;

/**
 * The WriteLog interface can be implemented by a class that is designed to write a log to a destination
 * within AWS.
 */
public interface WriteLog {
  
  /**
   * Takes an object of type File and writes it to a file with the filename.
   * @param file of type File
   * @param filename for the created file.
   */
  void writeLog(File file, String filename) throws IOException;
}
