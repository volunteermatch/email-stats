package org.vm.email.cleanup;

import org.vm.shared.SharedCSVWriter;

/**
 * This class inherits from the SharedCSVWriter abstract class, which inherits methods from SharedFileWriter
 * class. The CleanCSVWriter is the implementation of a CSVWriter specifically for the Clean method.
 * Every line in the resulting csv file will contain information from mail log paired with one of it's sendgrid events.
 */
public class CleanCSVWriter extends SharedCSVWriter {
  
  /**
   * The constructor uses the SharedCSVWriter constructor and establishes that the comma
   * is the chosen value separator.
   * @param filePath for the new file
   * @param filename of the new file.
   */
  public CleanCSVWriter(String filePath, String filename) {
    super(filePath, filename, ',');
  }
  
  /**
   * The write to file method is used in Clean and takes in an Array of Strings containing
   * info to write as a line. Each time this method is called, it automatically writes a new line with the
   * given information and separates each entry in the array with a comma.
   * @param o in this case will be of type String[].
   */
  @Override
  public void writeToFile(Object o) {
    String[] fileInfo = checkForCommas((String[]) o);
    super.writeNextLine(fileInfo);
  }
  
  /**
   * This method is called each time a new line is written to the file and checks whether or not
   * a comma is present in any of the entries. If it is, it will escape that entry by surrounding it in quotes.
   * @param fileInfo is array of info to be written to CSV.
   * @return the same array but with all entries with commas properly escaped.
   */
  public String[] checkForCommas(String[] fileInfo) {
    for (int i = 0; i < fileInfo.length; i++) {
      if (fileInfo[i] != null) {
        if (fileInfo[i].contains(",")) {
          fileInfo[i] = "\"" + fileInfo[i] + "\"";
        }
      }
    }
    return fileInfo;
  }
  
  /**
   * This header is derived from the columns in the database the Clean method accesses.
   */
  @Override
  public void writeCSVHeader() {
    String[] header = new String[28];
    header[0] = "ml_id";
    header[1] = "ml_email";
    header[2] = "ml_guid";
    header[3] = "ml_host";
    header[4] = "ml_ref1";
    header[5] = "ml_ref2";
    header[6] = "ml_sent_time";
    header[7] = "ml_type";
    header[8] = "se_sg_event_id";
    header[9] = "se_asm_group_id";
    header[10] = "se_attempt";
    header[11] = "se_category";
    header[12] = "se_email";
    header[13] = "se_event";
    header[14] = "se_ip";
    header[15] = "se_reason";
    header[16] = "se_response";
    header[17] = "se_sg_message_id";
    header[18] = "se_smtp_id";
    header[19] = "se_status";
    header[20] = "se_timestamp";
    header[21] = "se_tls";
    header[22] = "se_type";
    header[23] = "se_unsubscribe_url";
    header[24] = "se_url";
    header[25] = "se_url_offset";
    header[26] = "se_useragent";
    header[27] = "se_vm_timestamp";
    super.writeNextLine(header);
  
  }
}
