package org.vm.email.cleanup;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.vm.shared.S3LogWriter;
import org.vm.shared.SharedFileWriter;

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
            
            //Get basename for file.
            String baseName = System.getenv("BASE_FILENAME");
            
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
            
            //Sets up variables used for creating new files. The SharedFileWriter is an abstract class that allows for
            //the writing of various file types. In this case it is a CSV file.
            SharedFileWriter writer = null;
            
            //This will be the Timestamp of the first log from the queried section of the database and will be used for
            //naming the CSV file.
            String firstTimestamp = null;
            
            //The bucketWriter is used to write newly created files to the desired S3 bucket.
            S3LogWriter bucketWriter = new S3LogWriter(bucketName);
            
            //Will increment as new files are created and added on to the file name.
            int fileNumber = 1;
            
            //Tracks how many rows are deleted in both mail_log and sendgrid_event tables.
            int numDeleted = 0;
    
            //Loop through all emails from before selected date of a certain type and add their info to a CSV file.
            String query = "SELECT * FROM mail_log WHERE sent_time < ? AND type != 'custom_email_to_volunteer' ORDER BY sent_time ASC";
            PreparedStatement statement = con.prepareStatement(query);
            statement.setTimestamp(1, dateX);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                //If no file has been created yet, create the first file.
                if (writer == null) {
                    firstTimestamp = tsToString(rs.getTimestamp("sent_time"));
                    writer = new CleanCSVWriter("/tmp/",baseName + firstTimestamp + "_" + fileNumber);
                
                //If a file has already been created but it has recorded the max number of emails, write file
                //to S3 bucket and create a new file to write to.
                } else if (maxEmailNum == 0) {
                    writer.flushAndClose();
                    bucketWriter.writeLog(writer.getFile(), writer.getFile().getName());
                    writer.deleteFile();
                    int numFromDeletion = cleanDelete(con, guidList);
                    numDeleted = numDeleted + numFromDeletion;
                    guidList = new ArrayList<>();
                    fileNumber ++;
                    writer = new CleanCSVWriter("/tmp/", baseName + firstTimestamp + "_" + fileNumber);
                    maxEmailNum = Integer.parseInt(emailNum);
                }
                //Decrements the maxEmailNum to track how many emails have been recorded in the current file.
                maxEmailNum --;
    
                //Creates Array of strings with info from the mail log.
                String[] mailInfo = new String[8];
                mailInfo[0] = (Integer.toString(rs.getInt("id")));
                mailInfo[1] = (rs.getString("email"));
                mailInfo[2] = (rs.getString("guid"));
                mailInfo[3] = (rs.getString("host"));
                mailInfo[4] = (rs.getString("ref1"));
                mailInfo[5] = (rs.getString("ref2"));
                mailInfo[6] = (tsToString(rs.getTimestamp("sent_time")));
                mailInfo[7] = (rs.getString("type"));
                
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
                    //Array of strings representing the given sendgrid event info associated with the mail guid.
                    String[] eventInfo = new String[20];
                    eventInfo[0] = (rs2.getString("sg_event_id"));
                    eventInfo[1] = (Integer.toString(rs2.getInt("asm_group_id")));
                    eventInfo[2] = (rs2.getString("attempt"));
                    eventInfo[3] = (rs2.getString("category"));
                    eventInfo[4] = (rs2.getString("email"));
                    eventInfo[5] = (rs2.getString("event"));
                    eventInfo[6] = (rs2.getString("ip"));
                    eventInfo[7] = (rs2.getString("reason"));
                    eventInfo[8] = (rs2.getString("response"));
                    eventInfo[9] = (rs2.getString("sg_message_id"));
                    eventInfo[10] = (rs2.getString("smtp_id"));
                    eventInfo[11] = (rs2.getString("status"));
                    eventInfo[12] = (tsToString(rs2.getTimestamp("timestamp")));
                    eventInfo[13] = (Integer.toString(rs2.getInt("tls")));
                    eventInfo[14] = (rs2.getString("type"));
                    eventInfo[15] = (rs2.getString("unsubscribe_url"));
                    eventInfo[16] = (rs2.getString("url"));
                    eventInfo[17] = (rs2.getString("url_offset"));
                    eventInfo[18] = (rs2.getString("useragent"));
                    eventInfo[19] = (tsToString(rs2.getTimestamp("vm_timestamp")));
    
                    //Concatenates the two arrays to be write the entire line to the file.
                    int length = mailInfo.length + eventInfo.length;
                    String[] combined = new String[length];
                    int pos = 0;
                    for (String element : mailInfo) {
                        combined[pos] = element;
                        pos++;
                    }
                    for (String element : eventInfo) {
                        combined[pos] = element;
                        pos++;
                    }
                    writer.writeToFile(combined);
   
                }
                //If no events associated with a given email, it adds it to the CSV file with no events.
                if (!events) {
                    writer.writeToFile(mailInfo);
                }
                rs2.close();
            }
            rs.close();
            
            //Writer is null if there are no emails in the specified time frame that are recorded/deleted.
            if (writer != null) {
                writer.flushAndClose();
    
                //Writes the last log to S3 bucket.
                bucketWriter.writeLog(writer.getFile(), writer.getFile().getName());
                int numFromDeletion = cleanDelete(con, guidList);
                numDeleted = numDeleted + numFromDeletion;
                
                //Deletes all info of type custom_email_to_volunteer from specified timeframe.
                int numOfCustomToVolunteer = deleteCustomToVolunteer(con, dateX);
                
                //Total number of emails deleted.
                int combinedNum = numDeleted + numOfCustomToVolunteer;
                
                statement.close();
                con.close();
    
                String outputMessage = "SUCCESS: " + combinedNum + " rows deleted from database. " + numOfCustomToVolunteer + " Opp Alerts have been deleted and " + numDeleted + " rows of deleted information have been recorded in "
                               + bucketName + " in files titled " + baseName + firstTimestamp + ".csv";
                System.out.println(outputMessage);
    
                return outputMessage;
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
     * Deletes information from the database once it has been recorded.
     * @param con is the current connection.
     * @param guidList the list of guids that determine which info is ready to be deleted.
     * @return the number of emails deleted from this call of the method.
     * @throws SQLException in case of error.
     */
    public int cleanDelete(Connection con, List<String> guidList) throws SQLException {
        Statement stat = con.createStatement();
        stat.execute("SET FOREIGN_KEY_CHECKS=0");
    
        //Deletes recorded logs using a SQL array of the guids.
        String update = "DELETE FROM sendgrid_event WHERE guid IN (?)";
        String sqlIN = guidList.stream()
                           .map(String::valueOf)
                           .collect(Collectors.joining("','", "('", "')"));
        update = update.replace("(?)", sqlIN);
        PreparedStatement statement = con.prepareStatement(update);
        int numSendgrid = statement.executeUpdate();
    
        update = "DELETE FROM mail_log WHERE guid IN (?)";
        update = update.replace("(?)", sqlIN);
        statement = con.prepareStatement(update);
        int numMailLog = statement.executeUpdate();
    
        //Closes connections and statements.
        stat.execute("SET FOREIGN_KEY_CHECKS=1");
        statement.close();
        stat.close();
        return numSendgrid + numMailLog;
    }
    
    /**
     * Deletes all emails of type "custom_email_to_volunteer" from the database before specified date.
     * @param con is the current connection.
     * @param dateX is the date determining which files are deleted.
     * @return The total number of rows deleted from this operation.
     * @throws SQLException in case of error.
     */
    public int deleteCustomToVolunteer(Connection con, Timestamp dateX) throws SQLException {
        Statement stat = con.createStatement();
        stat.execute("SET FOREIGN_KEY_CHECKS=0");
        
        String update = "DELETE mail_log, sendgrid_event FROM mail_log INNER JOIN sendgrid_event " +
                            "ON mail_log.guid = sendgrid_event.guid WHERE " +
                            "mail_log.type = 'custom_email_to_volunteer' AND sent_time < ?";
        PreparedStatement statement = con.prepareStatement(update);
        statement.setTimestamp(1, dateX);
        int addNumDeleted = statement.executeUpdate();
    
        stat.execute("SET FOREIGN_KEY_CHECKS=1");
        statement.close();
        stat.close();
        return addNumDeleted;
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
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("PST"));
        Date date = new Date();
        date.setTime(ts.getTime());
        return sdf.format(date);
    }
    
}
