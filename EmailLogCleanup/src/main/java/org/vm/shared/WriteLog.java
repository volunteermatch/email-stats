package org.vm.shared;

import java.io.InputStream;

/**
 * The WriteLog interface can be implemented by a class that is designed to write a log to a destination
 * within AWS.
 */
public interface WriteLog {
  
  /**
   * Takes an object of type InputStream and writes it to a file with the filename.
   * @param stream of type InputStream
   * @param filename for the created file.
   */
  void writeLog(InputStream stream, String filename);
}
