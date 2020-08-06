package org.vm.email.cleanup;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
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
    
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    
            String query = "SELECT * FROM mail_log WHERE timestamp < ? AND type != custom_email_to_volunteer";
            PreparedStatement statement = con.prepareStatement(query);
            statement.setTimestamp(1, dateX);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                JSONObject mailInfo = new JSONObject();
                mailInfo.put("id", rs.getInt("id"));
                mailInfo.put("email", rs.getString("email"));
                mailInfo.put("guid", rs.getString("guid"));
                mailInfo.put("host", rs.getString("host"));
                mailInfo.put("ref1", rs.getString("ref1"));
                mailInfo.put("ref2", rs.getString("ref2"));
                mailInfo.put("sent_time", rs.getTimestamp("sent_time"));
                mailInfo.put("type", rs.getString("type"));
    
                JSONArray eventArray = new JSONArray();
                query = "SELECT * FROM sendgrid_event WHERE sendgrid_event.guid = ?";
                statement = con.prepareStatement(query);
                String currGuid = rs.getString("guid");
                statement.setString(1, currGuid);
                ResultSet rs2 = statement.executeQuery();
                while (rs2.next()) {
                    JSONObject eventInfo = new JSONObject();
                    eventInfo.put("sg_event_id", rs2.getString("sg_event_id"));
                    eventInfo.put("asm_group_id", rs2.getInt("asm_group_id"));
                    eventInfo.put("attempt", rs2.getString("attempt"));
                    eventInfo.put("category", rs2.getString("category"));
                    eventInfo.put("email", rs2.getString("email"));
                    eventInfo.put("event", rs2.getString("event"));
                    eventInfo.put("guid", currGuid);
                    eventInfo.put("ip", rs2.getString("ip"));
                    eventInfo.put("reason", rs2.getString("reason"));
                    eventInfo.put("response", rs2.getString("response"));
                    eventInfo.put("sg_message_id", rs2.getString("sg_message_id"));
                    eventInfo.put("smtp_id", rs2.getString("smtp_id"));
                    eventInfo.put("status", rs2.getString("status"));
                    eventInfo.put("timestamp", rs2.getTimestamp("timestamp"));
                    eventInfo.put("tls", rs2.getInt("tls"));
                    eventInfo.put("type", rs2.getString("type"));
                    eventInfo.put("unsubscribe_url", rs2.getString("unsubscribe_url"));
                    eventInfo.put("url", rs2.getString("url"));
                    eventInfo.put("url_offset", rs2.getString("url_offset"));
                    eventInfo.put("useragent", rs2.getString("useragent"));
                    eventInfo.put("vm_timestamp", rs2.getTimestamp("vm_timestamp"));
                    
                    eventArray.add(eventInfo);
                }
                mailInfo.put("sendgrid_events", eventArray);
                WriteLog(mailInfo, currGuid, s3Client);
            }
            
            
            //Code used for deleting entries and updating database.
            Statement stat = con.createStatement();
            stat.execute("SET FOREIGN_KEY_CHECKS=0");
            String update = "DELETE FROM sendgrid_event WHERE timestamp < ?";
            statement = con.prepareStatement(update);
            statement.setTimestamp(1, dateX);
            int numSendgrid = statement.executeUpdate();
            
            update = "DELETE FROM mail_log WHERE sent_time < ?";
            statement = con.prepareStatement(update);
            statement.setTimestamp(1, dateX);
            int numMailLog = statement.executeUpdate();
            
            //Closes connections and statements.
            stat.execute("SET FOREIGN_KEY_CHECKS=1");
            statement.close();
            stat.close();
            con.close();
            
            System.out.println("SUCCESS: " + numSendgrid + " rows deleted from sendgrid_event table and " + numMailLog +
                                   " rows deleted from mail_log table");
            
            return "SUCCESS: " + numSendgrid + " rows deleted from sendgrid_event table and " + numMailLog +
                       " rows deleted from mail_log table";
        } catch (ClassNotFoundException | SQLException | ParseException e) {
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
    
    @Override
    public void WriteLog(JSONObject jsonObject, String filename, AmazonS3 s3Client) {
        try (FileWriter file = new FileWriter(filename + ".json")) {
            
            PutObjectRequest request = new PutObjectRequest("bucketName", "fileObjKeyName", jsonObject.toJSONString());
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("title", "someTitle");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        
        } catch (IOException | SdkClientException e) {
            e.printStackTrace();
        }
    
    }
}
