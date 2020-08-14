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
            File file = new File("/tmp/" + filename + ".csv");
            if (file.createNewFile()) {
                System.out.println("File Created: " + file.getName());
                System.out.println("Path: " + file.getAbsolutePath());
            } else {
                System.out.println("ERROR: Something wrong with writing file creation");
            }
            
            //Setup guid list to loop through.
            List<String> guidList = new ArrayList<>();
            
            //Sets up the csvWriter and the header for the csv.
            FileWriter csvWriter = new FileWriter("/tmp/" + filename + ".csv");
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
            
            //Creates an instance of the S3LogWriterClass and writes the file to the s3
            S3LogWriter logWriter = new S3LogWriter(file, bucketName);
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
