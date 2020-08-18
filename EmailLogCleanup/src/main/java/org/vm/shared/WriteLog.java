package org.vm.shared;

import java.io.File;

/**
 * The WriteLog interface can be implemented by a class that is designed to write a log to a destination
 * within AWS.
 */
public interface WriteLog {
  
  void writeLog(File file);
}
