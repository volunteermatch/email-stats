package org.vm.email.cleanup;

import org.vm.shared.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CleanCSVWriter extends CSVWriter {
  
  /**
   * The constructor creates the new desired file, a file writer to write to the new file, and a writes
   * the header for the file.
   * @param filePath for the new file
   * @param filename of the new file.
   */
  public CleanCSVWriter(String filePath, String filename) {
    super(filePath, filename);
  }
  
  @Override
  public void writeToFile(Object o) {
    List<List<String>> fileInfo = (List<List<String>>) o;
    super.writeString(String.join(",", fileInfo.get(0)));
    if (fileInfo.get(1).size() > 0) {
      super.addComma();
      super.writeString(String.join(",", fileInfo.get(1)));
    }
    super.newLine();
  }
  
  @Override
  public void writeCSVHeader(FileWriter csvWriter) {
    try {
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
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
