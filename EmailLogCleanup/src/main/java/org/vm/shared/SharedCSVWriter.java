package org.vm.shared;

import com.opencsv.CSVWriter;

/**
 * This abstract class that inhertis from the SharedFileWriter class. It is designed for creating
 * CSV files with a defined method for writing a new line. It makes use of the opencsv library,
 * the documentation for which can be found here: http://opencsv.sourceforge.net.
 */
public abstract class SharedCSVWriter extends SharedFileWriter {
  
  private final CSVWriter writer;
  
  /**
   * The constructor creates the new desired file, a file writer to write to the new file, and a writes
   * the header for the file. It also establishes the value separator for the csv file.
   * @param filePath is where the newly created file will go.
   * @param filename of the new file.
   * @param valueSeparator is the configurable delimiter for the file.
   */
  public SharedCSVWriter(String filePath, String filename, char valueSeparator) {
    super(filePath, filename, ".csv");
    writer = new CSVWriter(super.getFileWriter(), valueSeparator);
    writeCSVHeader();
  }
  
  /**
   * Uses the CSVWriter classes method for writing a new line to a CSV file when passed in as an array of strings.
   * @param line of type String[] with info to be written to csv file.
   */
  public void writeNextLine(String[] line) {
    writer.writeNext(line);
  }
  
  /**
   * Object will contain information in a format specific to the program.
   * @param o containing information to parse into file.
   */
  @Override
  public abstract void writeToFile(Object o);
  
  /**
   * A helper method that prepares the CSV file header.
   */
  public abstract void writeCSVHeader();
  
}
