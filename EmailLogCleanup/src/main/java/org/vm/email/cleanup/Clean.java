package org.vm.email.cleanup;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.vm.shared.S3LogWriter;

/**
 * Handler for requests to Lambda function.
 */
public class Clean implements RequestHandler<Object, String> {
    
    /**
     * The body of the Lambda function. Takes in a basic string input (per requirements of RequestHandler
     * interface). Connects email_db based on specifications from environment variables and deletes
     * entries in sendgrid_event and mail_log tables logged earlier than the specified number days earlier.
     * @param input is the Lambda event.
     * @param context a component of the input as processed by a RequestHandler interface.
     * @return A string showing the success of the function once it has been running.
     */
    public String handleRequest(final Object input, final Context context)  {
        try {
            //Get desired timestamp to refer to for database update.
            String offset = System.getenv("OFFSET");
            Timestamp dateX = convertToTimestamp(offset);
            
            //Retrieve the number of emails to be logged to each created file.
            String emailNum = System.getenv("EMAILNUM");
            int maxEmailNum = Integer.parseInt(emailNum);
            
            //Retrieve name of destination bucket for CSV log.
            String bucketName = System.getenv("BUCKET");
            
            //Get information for accessing database from environment variables.
            String dbName = System.getenv("DBNAME");
            String dbUser = System.getenv("DBUSER");
            String dbPass = System.getenv("DBPASSWORD");
            String port = System.getenv("PORT");
            String hostname = System.getenv("HOSTNAME");
            
            //Establish connection.
            Class.forName("com.mysql.cj.jdbc.Driver");
            String jdbc = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName+ "?user=" + dbUser + "&password=" + dbPass;
            Connection con = DriverManager.getConnection(jdbc);
            
            //Setup guid list to loop through.
            List<String> guidList = new ArrayList<>();
            
            //Sets up variables used for creating new files.
            Writer writer = null;
            FileWriter csvWriter = null;
            String firstTimestamp = null;
            S3LogWriter logWriter = null;
            int fileNumber = 1;
     
            //Loop through all emails from before selected date of a certain type and add their info to a CSV file.
            String query = "SELECT * FROM mail_log WHERE sent_time < ? AND type != 'custom_email_to_volunteer' ORDER BY sent_time ASC";
            PreparedStatement statement = con.prepareStatement(query);
            statement.setTimestamp(1, dateX);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                //If no file has been created yet, create the first file.
                if (csvWriter == null) {
                    firstTimestamp = tsToString(rs.getTimestamp("sent_time"));
                    writer = new Writer(firstTimestamp + "_" + fileNumber);
                    csvWriter = writer.getFileWriter();
                
                //If a file has already been created but it has recorded the max number of emails, write file
                //to S3 bucket and create a new file to write to.
                } else if (maxEmailNum == 0) {
                    csvWriter.flush();
                    csvWriter.close();
                    logWriter = new S3LogWriter(writer.getFile(), bucketName);
                    logWriter.writeLog();
                    writer.deleteFile();
                    fileNumber ++;
                    writer = new Writer(firstTimestamp + "_" + fileNumber);
                    csvWriter = writer.getFileWriter();
                    maxEmailNum = Integer.parseInt(emailNum);
                }
                //Decrements the maxEmailNum to track how many emails have been recorded in the current file.
                maxEmailNum --;
    
                List<String> mailInfo = new ArrayList<>();
                mailInfo.add(Integer.toString(rs.getInt("id")));
                mailInfo.add(rs.getString("email"));
                mailInfo.add(rs.getString("guid"));
                mailInfo.add(rs.getString("host"));
                mailInfo.add(rs.getString("ref1"));
                mailInfo.add(rs.getString("ref2"));
                mailInfo.add(tsToString(rs.getTimestamp("sent_time")));
                mailInfo.add(rs.getString("type"));
                
                //Query to find all sendgrid events associated with an email.
                query = "SELECT * FROM sendgrid_event WHERE sendgrid_event.guid = ?";
                statement = con.prepareStatement(query);
                String currGuid = rs.getString("guid");
                guidList.add(currGuid);
                statement.setString(1, currGuid);
                ResultSet rs2 = statement.executeQuery();
                
                //Boolean created to track whether or not a given email has associated events.
                boolean events = false;
                while (rs2.next()) {
                    events = true;
                    List<String> eventInfo = new ArrayList<>();
                    eventInfo.add(rs2.getString("sg_event_id"));
                    eventInfo.add(Integer.toString(rs2.getInt("asm_group_id")));
                    eventInfo.add(rs2.getString("attempt"));
                    eventInfo.add(rs2.getString("category"));
                    eventInfo.add(rs2.getString("email"));
                    eventInfo.add(rs2.getString("event"));
                    eventInfo.add(rs2.getString("ip"));
                    eventInfo.add(rs2.getString("reason"));
                    eventInfo.add(rs2.getString("response"));
                    eventInfo.add(rs2.getString("sg_message_id"));
                    eventInfo.add(rs2.getString("smtp_id"));
                    eventInfo.add(rs2.getString("status"));
                    eventInfo.add(tsToString(rs2.getTimestamp("timestamp")));
                    eventInfo.add(Integer.toString(rs2.getInt("tls")));
                    eventInfo.add(rs2.getString("type"));
                    eventInfo.add(rs2.getString("unsubscribe_url"));
                    eventInfo.add(rs2.getString("url"));
                    eventInfo.add(rs2.getString("url_offset"));
                    eventInfo.add(rs2.getString("useragent"));
                    eventInfo.add(tsToString(rs2.getTimestamp("vm_timestamp")));
                    
                    //Writes mail log and sendgrid event info into a new line in the CSV file.
                    csvWriter.append(String.join(",", mailInfo));
                    csvWriter.append(",");
                    csvWriter.append(String.join(",", eventInfo));
                    csvWriter.append("\n");
                }
                //If no events associated with a given email, it adds it to the CSV file with no events.
                if (!events) {
                    csvWriter.append(String.join(",", mailInfo));
                    csvWriter.append(",");
                    csvWriter.append("No events associated with this email");
                    csvWriter.append("\n");
                }
                rs2.close();
            }
            rs.close();
        
            //If CSV writer is not null, that means that entries were found and recorded and are ready to be deleted.
            if (csvWriter != null) {
                csvWriter.flush();
                csvWriter.close();
    
                //Writes the last log to S3 bucket.
                assert writer != null;
                logWriter = new S3LogWriter(writer.getFile(), bucketName);
                logWriter.writeLog();
                
                //Allows for deletions to occur regardless of references between tables.
                Statement stat = con.createStatement();
                stat.execute("SET FOREIGN_KEY_CHECKS=0");
    
                //Deletes recorded logs using a SQL array of the guids.
                String update = "DELETE FROM sendgrid_event WHERE guid IN (?)";
                String sqlIN = guidList.stream()
                                   .map(String::valueOf)
                                   .collect(Collectors.joining("','", "('", "')"));
                update = update.replace("(?)", sqlIN);
                statement = con.prepareStatement(update);
                int numSendgrid = statement.executeUpdate();
    
                update = "DELETE FROM mail_log WHERE guid IN (?)";
                update = update.replace("(?)", sqlIN);
                statement = con.prepareStatement(update);
                int numMailLog = statement.executeUpdate();
    
                //Uses a a select statement to see if there are any of info of type custom_email_to_volunteer.
                List<String> oppList = new ArrayList<>();
                query = "SELECT guid FROM mail_log WHERE sent_time < ? AND type = 'custom_email_to_volunteer'";
                statement = con.prepareStatement(query);
                statement.setTimestamp(1, dateX);
                ResultSet rs3 = statement.executeQuery();
                while (rs3.next()) {
                    oppList.add(rs3.getString("guid"));
                }
                rs3.close();
                //If there are inputs of custom_email_to_volunteer, delete them.
                if (!oppList.isEmpty()) {
                    update = "DELETE FROM mail_log WHERE sent_time < ? AND type = 'custom_email_to_volunteer'";
                    statement = con.prepareStatement(update);
                    statement.setTimestamp(1, dateX);
                    int mailLogAdd = statement.executeUpdate();
        
                    update = "DELETE FROM sendgrid_event WHERE guid IN (?)";
                    sqlIN = oppList.stream()
                                .map(String::valueOf)
                                .collect(Collectors.joining("','", "('", "')"));
                    update = update.replace("(?)", sqlIN);
                    statement = con.prepareStatement(update);
                    int sendGridAdd = statement.executeUpdate();
        
                    numMailLog = numMailLog + mailLogAdd;
                    numSendgrid = numSendgrid + sendGridAdd;
                }
    
                //Closes connections and statements.
                stat.execute("SET FOREIGN_KEY_CHECKS=1");
                statement.close();
                stat.close();
                con.close();
    
                System.out.println("SUCCESS: " + numSendgrid + " rows deleted from sendgrid_event table and " + numMailLog
                                       + " rows deleted from mail_log table. Deleted information has been recorded in "
                                       + bucketName + " in files titled emailEventStore" + firstTimestamp + ".csv");
    
                return "SUCCESS: " + numSendgrid + " rows deleted from sendgrid_event table and " + numMailLog
                           + " rows deleted from mail_log table. Deleted information has been recorded in "
                           + bucketName + " in files titled emailEventStore" + firstTimestamp + ".csv";
                
            //If the CSVwriter was null, that means no data fit the criteria to be deleted.
            } else {
                System.out.println("No data in the specified time frame. Nothing deleted or recorded from database");
                return "No data in the specified time frame. Nothing deleted or recorded from database";
            }
            
        } catch (ClassNotFoundException | SQLException | ParseException | IOException e) {
            e.printStackTrace();
            return "ERROR: Database connection error or parsing error";
        }
    }
    
    
    /**
     * Takes a number in the form of a string representing a number of days and returns
     * the date that occured given number of days before the current date in the form
     * of a timestamp.
     * @param offset as a String that represents a given number of days.
     * @return Timestamp of the target date.
     *
     */
    private Timestamp convertToTimestamp(String offset) throws ParseException {
        int offDays = Integer.parseInt(offset);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -offDays);
        java.util.Date date = cal.getTime();
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
        String stringDate = format1.format(date);
        java.util.Date dateJ = format1.parse(stringDate);
        java.sql.Date sDate = new java.sql.Date(dateJ.getTime());
        return new Timestamp(sDate.getTime());
    }
    
    /**
     * A helper method that turns a timestamp into a string for the CSV file.
     * @param ts Timestamp of email.
     * @return timestamp in a string format that mimics the database timestamp format.
     */
    private String tsToString(Timestamp ts) {
        Date date = new Date();
        date.setTime(ts.getTime());
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    }
    
}
