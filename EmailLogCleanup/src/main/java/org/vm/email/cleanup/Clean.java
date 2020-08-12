package org.vm.email.cleanup;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import org.vm.shared.WriteLog;

/**
 * Handler for requests to Lambda function.
 */
public class Clean implements RequestHandler<Object, String>, WriteLog {
    
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
            
            //Retrieve name for CSV file and Number of Emails desired.
            String filename = System.getenv("LOGNAME");
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
            
            //Creates new csv file to be written to.
            File file = new File(filename + ".csv");
            if (file.createNewFile()) {
                System.out.println("File Created: " + file.getName());
                System.out.println("Path: " + file.getAbsolutePath());
            } else {
                System.out.println("ERROR: Something wrong with writing file");
            }
            
            //Setup guid list to loop through.
            List<String> guidList = new ArrayList<>();
            
            //Sets up the csvWriter and the header for the csv.
            FileWriter csvWriter = new FileWriter(filename + ".csv");
            writeCSVHeader(csvWriter);
     
            //Loop through all emails from before selected date of a certain type and add their info to a CSV file.
            String query = "SELECT * FROM mail_log WHERE sent_time < ? AND type != 'custom_email_to_volunteer' ORDER BY sent_time ASC LIMIT ?";
            PreparedStatement statement = con.prepareStatement(query);
            statement.setTimestamp(1, dateX);
            statement.setInt(2, maxEmailNum);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                List<String> mailInfo = new ArrayList<>();
                mailInfo.add(Integer.toString(rs.getInt("id")));
                mailInfo.add(rs.getString("email"));
                mailInfo.add(rs.getString("guid"));
                mailInfo.add(rs.getString("host"));
                mailInfo.add(rs.getString("ref1"));
                mailInfo.add(rs.getString("ref2"));
                mailInfo.add(tsToString(rs.getTimestamp("sent_time")));
                mailInfo.add(rs.getString("type"));
                query = "SELECT * FROM sendgrid_event WHERE sendgrid_event.guid = ?";
                statement = con.prepareStatement(query);
                String currGuid = rs.getString("guid");
                guidList.add(currGuid);
                statement.setString(1, currGuid);
                ResultSet rs2 = statement.executeQuery();
                while (rs2.next()) {
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
                    csvWriter.append(String.join(",", mailInfo));
                    csvWriter.append(",");
                    csvWriter.append(String.join(",", eventInfo));
                    csvWriter.append("\n");
                }
                rs2.close();
            }
            rs.close();
            csvWriter.flush();
            csvWriter.close();
            WriteLog(file, bucketName);
    
            //Allows for deletions to occur regardless of references between tables.
            Statement stat = con.createStatement();
            stat.execute("SET FOREIGN_KEY_CHECKS=0");
            
            //Loops through saved guids from the recording phase and only deletes events that have been recorded.
            int numSendgrid = 0;
            for (String guid : guidList) {
                String update = "DELETE FROM sendgrid_event WHERE guid = ?";
                statement = con.prepareStatement(update);
                statement.setString(1, guid);
                int deletionNum = statement.executeUpdate();
                numSendgrid = numSendgrid + deletionNum;
                statement.close();
            }
            
            //Deletes all mail_log information before a certain date with the limit set to match which logs are recorded.
            String update = "DELETE FROM mail_log WHERE sent_time < ? AND type != 'custom_email_to_volunteer' ORDER BY sent_time ASC LIMIT ?";
            statement = con.prepareStatement(update);
            statement.setTimestamp(1, dateX);
            statement.setInt(2, maxEmailNum);
            int numMailLog = statement.executeUpdate();
            
            //Closes connections and statements.
            stat.execute("SET FOREIGN_KEY_CHECKS=1");
            statement.close();
            stat.close();
            con.close();
            
            System.out.println("SUCCESS: " + numSendgrid + " rows deleted from sendgrid_event table and " + numMailLog
                                   + " rows deleted from mail_log table. Deleted information has been recorded in "
                                   + bucketName + " in file " + filename + ".csv");
            
            return "SUCCESS: " + numSendgrid + " rows deleted from sendgrid_event table and " + numMailLog
                       + " rows deleted from mail_log table. Deleted information has been recorded in "
                       + bucketName + " in file " + filename + ".csv";
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
    
    /**
     * Method inherited from the WriteLog interface that will create a put request for the CSV file
     * into the s3 bucket.
     * @param file that is added to s3 bucket.
     * @param bucketName is the name of the destination s3 bucket.
     */
    @Override
    public void WriteLog(File file, String bucketName) {
    
        //Sets up the s3 bucket.
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        
        //Creates the PutObject request.
        try {
            PutObjectRequest request
                = new PutObjectRequest(bucketName, file.getName(), new File(file.getName()));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("title", "someTitle");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * A helper method that prepares the CSV file header.
     * @param csvWriter with nothing in it.
     * @throws IOException if I/O error occurs.
     */
    private void writeCSVHeader(FileWriter csvWriter) throws IOException {
        csvWriter.append("id");
        csvWriter.append(",");
        csvWriter.append("email");
        csvWriter.append(",");
        csvWriter.append("guid");
        csvWriter.append(",");
        csvWriter.append("host");
        csvWriter.append(",");
        csvWriter.append("ref1");
        csvWriter.append(",");
        csvWriter.append("ref2");
        csvWriter.append(",");
        csvWriter.append("sent_time");
        csvWriter.append(",");
        csvWriter.append("type");
        csvWriter.append(",");
        csvWriter.append("sg_event_id");
        csvWriter.append(",");
        csvWriter.append("asm_group_id");
        csvWriter.append(",");
        csvWriter.append("attempt");
        csvWriter.append(",");
        csvWriter.append("category");
        csvWriter.append(",");
        csvWriter.append("email");
        csvWriter.append(",");
        csvWriter.append("event");
        csvWriter.append(",");
        csvWriter.append("ip");
        csvWriter.append(",");
        csvWriter.append("reason");
        csvWriter.append(",");
        csvWriter.append("response");
        csvWriter.append(",");
        csvWriter.append("sg_message_id");
        csvWriter.append(",");
        csvWriter.append("smtp_id");
        csvWriter.append(",");
        csvWriter.append("status");
        csvWriter.append(",");
        csvWriter.append("timestamp");
        csvWriter.append(",");
        csvWriter.append("tls");
        csvWriter.append(",");
        csvWriter.append("type");
        csvWriter.append(",");
        csvWriter.append("unsubscribe_url");
        csvWriter.append(",");
        csvWriter.append("url");
        csvWriter.append(",");
        csvWriter.append("url_offset");
        csvWriter.append(",");
        csvWriter.append("useragent");
        csvWriter.append(",");
        csvWriter.append("vm_timestamp");
        csvWriter.append("\n");
    }
}
