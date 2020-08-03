package emailcleanup;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * Handler for requests to Lambda function.
 */
public class Clean implements RequestHandler<Object, String>{
    
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
            
            /*
            //Testing connection and code by using SELECT statements
            ArrayList<String> dateList1 = new ArrayList<>();
            Statement stat = con.createStatement();
            stat.execute("SET FOREIGN_KEY_CHECKS=0");
            String update = "SELECT timestamp FROM sendgrid_event WHERE timestamp < ?";
            PreparedStatement statement = con.prepareStatement(update);
            statement.setTimestamp(1, dateX);
            ResultSet rs = statement.executeQuery();
            while(rs.next()) {
                dateList1.add(rs.getTimestamp(1).toString());
            }
            System.out.println("Sendgrid Dates " + Arrays.toString(dateList1.toArray()));
            rs.close();
            
            ArrayList<String> dateList2 = new ArrayList<>();
            update = "SELECT sent_time FROM mail_log WHERE sent_time < ?";
            statement = con.prepareStatement(update);
            statement.setTimestamp(1, dateX);
            ResultSet rs2 = statement.executeQuery();
            while(rs2.next()) {
                dateList2.add(rs2.getTimestamp(1).toString());
            }
            System.out.println("Mail Log Dates " + Arrays.toString(dateList2.toArray()));
            rs2.close();
            int numSendgrid = 0;
            int numMailLog = 0;
             */
            
            //Actual function code, will be used for deleting entries and updating database.
            Statement stat = con.createStatement();
            stat.execute("SET FOREIGN_KEY_CHECKS=0");
            String update = "DELETE FROM sendgrid_event WHERE timestamp < ?";
            PreparedStatement statement = con.prepareStatement(update);
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
    
}
