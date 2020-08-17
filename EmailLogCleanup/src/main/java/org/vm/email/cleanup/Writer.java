package org.vm.email.cleanup;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class works as a wrapper class for the FileWriter and sets up the csv files
 * to be written to by the Clean class.
 */
public class Writer {
  
  private final FileWriter fw;
  private final File file;
  
  /**
   * The constructor creates the new desired file, a file writer to write to the new file, and a writes
   * the header for the file.
   * @param filename of the new file.
   * @throws IOException if there is an error creating the file.
   */
  public Writer(String filename) throws IOException {
    String baseName = System.getenv("BASE_FILENAME");
    file = new File("/tmp/" + baseName + filename + ".csv");
    if (file.createNewFile()) {
      System.out.println("File Created: " + file.getName());
    } else {
      System.out.println("ERROR: Something wrong with writing file creation");
    }
    
    fw = new FileWriter("/tmp/" + baseName + filename + ".csv");
    writeCSVHeader(fw);
  }
  
  /**
   * Returns the FileWriter of the given writer.
   * @return a FileWriter.
   */
  public FileWriter getFileWriter() {
    FileWriter csvWriter = fw;
    return csvWriter;
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
   * Deletes the file from the tmp folder in the lambda instance.
   */
  public void deleteFile() {
    if (file.delete()) {
      System.out.println(file.getName() + " deleted in Lambda instance");
    } else {
      System.out.println(file.getName() + " deletion failed");
    }
  }
  
  /**
   * A helper method that prepares the CSV file header.
   * @param csvWriter with nothing in it.
   * @throws IOException if I/O error occurs.
   */
  private void writeCSVHeader(FileWriter csvWriter) throws IOException {
    csvWriter.append("ml_id");
    csvWriter.append(",");
    csvWriter.append("ml_email");
    csvWriter.append(",");
    csvWriter.append("ml_guid");
    csvWriter.append(",");
    csvWriter.append("ml_host");
    csvWriter.append(",");
    csvWriter.append("ml_ref1");
    csvWriter.append(",");
    csvWriter.append("ml_ref2");
    csvWriter.append(",");
    csvWriter.append("ml_sent_time");
    csvWriter.append(",");
    csvWriter.append("ml_type");
    csvWriter.append(",");
    csvWriter.append("se_sg_event_id");
    csvWriter.append(",");
    csvWriter.append("se_asm_group_id");
    csvWriter.append(",");
    csvWriter.append("se_attempt");
    csvWriter.append(",");
    csvWriter.append("se_category");
    csvWriter.append(",");
    csvWriter.append("se_email");
    csvWriter.append(",");
    csvWriter.append("se_event");
    csvWriter.append(",");
    csvWriter.append("se_ip");
    csvWriter.append(",");
    csvWriter.append("se_reason");
    csvWriter.append(",");
    csvWriter.append("se_response");
    csvWriter.append(",");
    csvWriter.append("se_sg_message_id");
    csvWriter.append(",");
    csvWriter.append("se_smtp_id");
    csvWriter.append(",");
    csvWriter.append("se_status");
    csvWriter.append(",");
    csvWriter.append("se_timestamp");
    csvWriter.append(",");
    csvWriter.append("se_tls");
    csvWriter.append(",");
    csvWriter.append("se_type");
    csvWriter.append(",");
    csvWriter.append("se_unsubscribe_url");
    csvWriter.append(",");
    csvWriter.append("se_url");
    csvWriter.append(",");
    csvWriter.append("se_url_offset");
    csvWriter.append(",");
    csvWriter.append("se_useragent");
    csvWriter.append(",");
    csvWriter.append("se_vm_timestamp");
    csvWriter.append("\n");
  }
}
