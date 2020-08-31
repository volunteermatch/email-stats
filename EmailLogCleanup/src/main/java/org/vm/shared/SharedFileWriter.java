package org.vm.shared;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * The SharedFileWriter abstract class is designed to create a file of any type
 * and a file writer for the new file. This type is used in the Clean method, so that if VolunteerMatch
 * decides creating a JSON file would be more useful than a CSV file, all one needs to do is create
 * a JSON file writer that inherits from this class.
 *
 */
public abstract class SharedFileWriter {
  
  private final FileWriter fw;
  private final File file;
  
  /**
   * The constructor creates the new desired file and a file writer to write to the new file.
   * @param filePath for the new file.
   * @param filename of the new file.
   * @param fileType the type of the new file. Ex: .csv, .json...
   */
  public SharedFileWriter(String filePath, String filename, String fileType) {
    file = createFile(filePath + filename + fileType);
    fw = createFileWriter(filePath + filename + fileType);
  }
  
  /**
   * The file associated with the Writer.
   * @return a file.
   */
  public File getFile() {
    File currFile = file;
    return currFile;
  }
  
  /**
   * Access the file writer for this file.
   * @return the file writer.
   */
  public FileWriter getFileWriter() {
    return fw;
  }
  
  /**
   * Deletes the file from the instance of the code running.
   */
  public void deleteFile() {
    if (file.delete()) {
      System.out.println(file.getName() + " deleted");
    } else {
      System.out.println(file.getName() + " deletion failed");
    }
  }
  
  /**
   * Creates a new file using input filepath, filename, and file type.
   * @param filePath of file.
   * @return new file.
   */
  public File createFile(String filePath) {
    try {
      File file = new File(filePath);
      if (file.createNewFile()) {
        System.out.println("File Created: " + file.getName());
      } else {
        System.out.println("File already exists");
      }
      return file;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Creates new FileWriter for the specified file.
   * @param filePath of file.
   * @return FileWriter associated with File.
   */
  public FileWriter createFileWriter(String filePath) {
    try {
      return new FileWriter(filePath);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Flushes all written information and closes the FileWriter.
   */
  public void flushAndClose() {
    try {
      fw.flush();
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Writes input string to file.
   * @param info with desired String
   */
  public void writeInfo(String info) {
    try {
      fw.append(info);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * This method is to write anything into the file. It takes in an object to anticipate the wide variety of
   * possibilities for how your program specific FileWriter may want to parse information.
   * @param o containing information to parse into file.
   */
  public abstract void writeToFile(Object o);
  
}
