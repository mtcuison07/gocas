package org.rmj.car;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.lib.net.WebClient;
import org.rmj.replication.utility.MiscReplUtil;
import org.rmj.replication.utility.LogWrapper;

public class GOCAS_Save {
    public static void main(String [] args){
        LogWrapper logwrapr = new LogWrapper("GOCAS_Save", "gocas.log");
        
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        
        System.setProperty("sys.default.path.config", path);
        
        GRiderX instance = new GRiderX("IntegSys");
        
        if (!instance.logUser("IntegSys", "M001111122")){
            logwrapr.severe(instance.getMessage() + instance.getErrMsg());
            System.exit(1);
        }
        
        String lsSQL = "SELECT sValuexxx" + 
                        " FROM xxxOtherConfig" + 
                        " WHERE sConfigID = 'WebSvr'" + 
                            " AND `sProdctID` = 'IntegSys'";
        
        ResultSet loRS = instance.executeQuery(lsSQL);
        
        try {
            if (!loRS.next()){
                logwrapr.severe("Web server url is not set.");
                System.exit(1);
            }
        } catch (SQLException e) {
            logwrapr.severe(e.getMessage());
            System.exit(1);
        }
        
        Map<String, String> headers = getHeader();
        
        String response;
        
        try {
            String lsValue = MiscReplUtil.fileRead(System.getProperty("sys.default.path.config") + "/temp/car.tmp");
            
            JSONParser loParser = new JSONParser();
            JSONObject loJSON = (JSONObject) loParser.parse(lsValue);
            
            loJSON.put("sBranchCd", instance.getBranchCode());
            loJSON.put("sCreatedx", args[0]);
            
            JSONObject loJSONx = (JSONObject) loParser.parse(loJSON.get("applicant_info").toString());
            lsValue = (String) loJSONx.get("sLastName") + ", " +
                        (String) loJSONx.get("sFrstName");
            
            if (!"".equals((String) loJSONx.get("sSuffixNm")) && loJSONx.get("sSuffixNm") != null){
                lsValue += " " + (String) loJSONx.get("sSuffixNm");
            }
            
            if (!"".equals((String) loJSONx.get("sMiddName")) && loJSONx.get("sMiddName") != null){
                lsValue += " " + (String) loJSONx.get("sMiddName");
            }
            
            loJSON.put("sClientNm", lsValue);
            
            if (args[1].equals("0")){
                //lsSQL = "http://localhost/integsys/gocas/gocas_save_desktop.php";
                lsSQL = loRS.getString("sValuexxx") + "integsys/gocas/gocas_save_desktop.php";
            } else {
                loJSON.put("sTransNox", args[1]);
                //lsSQL = "http://localhost/integsys/gocas/gocas_update_desktop.php";
                lsSQL = loRS.getString("sValuexxx") + "integsys/gocas/gocas_update_desktop.php";
            }
                        
            response = WebClient.sendHTTP(lsSQL, loJSON.toJSONString(), (HashMap<String, String>) headers);

            if(response == null){
                System.out.println("No Response");
                System.exit(1);
            } 
            
            loJSON = (JSONObject) loParser.parse(response);
            
            if (!"success".equals((String) loJSON.get("result"))){
                loJSON = (JSONObject) loJSON.get("error");
                loJSON = (JSONObject) loParser.parse(loJSON.toJSONString());

                if ((int) (long) loJSON.get("code") == 40004){
                    System.exit(2);
                }
                
                System.exit(1);
            }
            
            lsSQL = (String) loJSON.get("sql");            
            instance.executeUpdate(lsSQL);
            
            System.exit(0);
        } catch (IOException | ParseException |SQLException ex) {
            System.out.println(ex.getMessage());
            System.exit(1);
        }
    }
    
    public static HashMap getHeader(){
        String clientid = "GGC_BM001";
        String productid = "IntegSys";
        String imei = "GMC_SEG09";
        String user = "M001111122";
        String log = "";
        
        Calendar calendar = Calendar.getInstance();
        Map<String, String> headers = 
                        new HashMap<String, String>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        headers.put("g-api-id", productid);
        headers.put("g-api-imei", imei);
        
        headers.put("g-api-key", SQLUtil.dateFormat(calendar.getTime(), "yyyyMMddHHmmss"));        
        headers.put("g-api-hash", org.apache.commons.codec.digest.DigestUtils.md5Hex((String)headers.get("g-api-imei") + (String)headers.get("g-api-key")));
        headers.put("g-api-client", clientid);    
        headers.put("g-api-user", user);    
        headers.put("g-api-log", log);    
        headers.put("g-char-request", "UTF-8");
        headers.put("g-api-token", "");    
        
        return (HashMap) headers;
    }
}