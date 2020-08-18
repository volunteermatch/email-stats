package org.vm.shared;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class CSVWriter implements SharedFileWriter{
  
  private final FileWriter fw;
  private final File file;
  
  /**
   * The constructor creates the new desired file, a file writer to write to the new file, and a writes
   * the header for the file.
   * @param filename of the new file.
   */
  public CSVWriter(String filePath, String filename) {
    file = createFile(filePath + filename + ".csv");
    fw = createFileWriter(filePath + filename + ".csv");
    writeCSVHeader(fw);
  }
  
  public void writeString(String info) {
    try {
      fw.append(info);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void addComma() {
    try {
      fw.append(",");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void newLine() {
    try {
      fw.append("\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * The file associated with the Writer.
   * @return a file.
   */
  @Override
  public File getFile() {
    File currFile = file;
    return currFile;
  }
  
  /**
   * Deletes the file from the tmp folder in the lambda instance.
   */
  @Override
  public void deleteFile() {
    if (file.delete()) {
      System.out.println(file.getName() + " deleted");
    } else {
      System.out.println(file.getName() + " deletion failed");
    }
  }
  
  @Override
  public File createFile(String filePath) {
    try {
      File file = new File(filePath);
      if (file.createNewFile()) {
        System.out.println("File Created: " + file.getName());
      } else {
        System.out.println("ERROR: Something wrong with writing file creation");
      }
      return file;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  @Override
  public FileWriter createFileWriter(String filePath) {
    try {
      return new FileWriter(filePath);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  @Override
  public void flushAndClose() {
    try {
      fw.flush();
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public abstract void writeToFile(Object o);
  
  /**
   * A helper method that prepares the CSV file header.
   * @param csvWriter with nothing in it.
   * @throws IOException if I/O error occurs.
   */
  public abstract void writeCSVHeader(FileWriter csvWriter);
  
}
