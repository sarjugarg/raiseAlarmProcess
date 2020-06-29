package com.gl.alarm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.gl.alarm.configuration.ConnectionConfiguration;


@SpringBootApplication
public class AlarmApplication {
	private static final Logger logger = Logger.getLogger(com.gl.alarm.AlarmApplication.class);
	static ConnectionConfiguration connectionConfiguration = null;
	  
	public static void main(String[] args) {
		String alertCode=args[0];
		String alertMessage=args[1];
		String alertFrom=args[2];
		SpringApplication.run(AlarmApplication.class, args);
		Map<String, String> placeholderMapForAlert = new HashMap<>();
	      placeholderMapForAlert.put("<e>",alertMessage);
	      placeholderMapForAlert.put("<process_name>", alertFrom);
	      raiseAlert(alertCode, placeholderMapForAlert, Integer.valueOf(0));
	      logger.info("Alert "+alertCode+" is raised. So, doing nothing.");
	}
	
	  public static String getAlertbyId(String alertId) {
		    Connection conn = null;
		    Statement stmt = null;
		    String description = "";
		    try {
		      conn = connectionConfiguration.getConnection();
		      stmt = conn.createStatement();
		      String sql = "select description from alert_db where alert_id='" + alertId + "'";
		      logger.info("Fetching alert message by alert id from alertDb");
		      ResultSet rs = stmt.executeQuery(sql);
		      while (rs.next())
		        description = rs.getString("description"); 
		    } catch (SQLException e) {
		      Map<String, String> placeholderMapForAlert = new HashMap<>();
		      placeholderMapForAlert.put("<e>", e.toString());
		      placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		      raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		      logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		      System.exit(0);
		    } catch (Exception e) {
		      Map<String, String> placeholderMapForAlert = new HashMap<>();
		      placeholderMapForAlert.put("<e>", e.toString());
		      placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		      raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		      logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		      System.exit(0);
		    } finally {
		      try {
		        if (stmt != null)
		          conn.close(); 
		      } catch (SQLException e) {
		        Map<String, String> placeholderMapForAlert = new HashMap<>();
		        placeholderMapForAlert.put("<e>", e.toString());
		        placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		        raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		        logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		        System.exit(0);
		      } 
		      try {
		        if (conn != null)
		          conn.close(); 
		      } catch (SQLException e) {
		        Map<String, String> placeholderMapForAlert = new HashMap<>();
		        placeholderMapForAlert.put("<e>", e.toString());
		        placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		        raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		        logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		        System.exit(0);
		      } 
		    } 
		    return description;
		  }
		  
		  public static void raiseAlert(String alertId, Map<String, String> bodyPlaceHolderMap, Integer userId) {
		    Connection conn = null;
		    Statement stmt = null;
		    try {
		      conn = connectionConfiguration.getConnection();
		      stmt = conn.createStatement();
		      String alertDescription = getAlertbyId(alertId);
		      if (Objects.nonNull(bodyPlaceHolderMap))
		        for (Map.Entry<String, String> entry : bodyPlaceHolderMap.entrySet()) {
		          logger.info("Placeholder key : " + (String)entry.getKey() + " value : " + (String)entry.getValue());
		          alertDescription = alertDescription.replaceAll(entry.getKey(), entry.getValue());
		        }  
		      logger.info("alert message: " + alertDescription);
		      String sql = "Insert into running_alert_db(alert_id,created_on,modified_on,description,status,user_id)values('" + alertId + "'," + defaultNowDate() + "," + defaultNowDate() + ",'" + alertDescription + "',0," + userId + ")";
		      logger.info("Inserting alert into running alert db");
		      stmt.executeQuery(sql);
		    } catch (SQLException e) {
		      Map<String, String> placeholderMapForAlert = new HashMap<>();
		      placeholderMapForAlert.put("<e>", e.toString());
		      placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		      raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		      logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		      System.exit(0);
		    } catch (Exception e) {
		      Map<String, String> placeholderMapForAlert = new HashMap<>();
		      placeholderMapForAlert.put("<e>", e.toString());
		      placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		      raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		      logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		      System.exit(0);
		    } finally {
		      try {
		        if (stmt != null)
		          conn.close(); 
		      } catch (SQLException e) {
		        Map<String, String> placeholderMapForAlert = new HashMap<>();
		        placeholderMapForAlert.put("<e>", e.toString());
		        placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		        raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		        logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		        System.exit(0);
		      } 
		      try {
		        if (conn != null)
		          conn.close(); 
		      } catch (SQLException e) {
		        Map<String, String> placeholderMapForAlert = new HashMap<>();
		        placeholderMapForAlert.put("<e>", e.toString());
		        placeholderMapForAlert.put("<process_name>", "Alarm.jar");
		        raiseAlert("alert006", placeholderMapForAlert, Integer.valueOf(0));
		        logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
		        System.exit(0);
		      } 
		    } 
		  }

		  public static String defaultNowDate() {
			    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSSSS");
			    String val = sdf.format(new Date());
			    String date = "to_timestamp('" + val + "','YYYY-MM-DD HH24:MI:SSFF5')";
			    return date;
			  }
}
