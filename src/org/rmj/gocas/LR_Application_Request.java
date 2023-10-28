package org.rmj.gocas;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.gocas.service.GOCASRestAPI;
import org.rmj.replication.utility.LogWrapper;

public class LR_Application_Request {
    public static void main(String [] args){
        LogWrapper logwrapr = new LogWrapper("LR_Application_Request", "ARSales.log");
        
        GRiderX _instance = new GRiderX("IntegSys");
        
        if(!_instance.getErrMsg().isEmpty()){
            logwrapr.severe(_instance.getErrMsg());
            logwrapr.severe("GRiderX has error...");
            System.exit(1);
        }
        
        _instance.setOnline(false);
        
        //get the transaction number of transferred applications that are on record
        String lsSQL = "SELECT sTransNox FROM MC_Credit_Application" +
                        " WHERE cTranStat = '2' AND sBranchCd = " + SQLUtil.toSQL(_instance.getBranchCode());
        
        ResultSet loRS = _instance.executeQuery(lsSQL);
        
        try {
            lsSQL = "";
            while (loRS.next()){
                lsSQL += "»" + SQLUtil.toSQL(loRS.getString("sTransNox"));
            }
            
            if (!lsSQL.isEmpty()) {
                lsSQL = lsSQL.substring(1);
                lsSQL = lsSQL.replace("»", ", ");
                lsSQL = "(" + lsSQL + ")";
                            
            }
            
            logwrapr.info("Start of process.");
            logwrapr.info("Retreiving info except these existing transaction numbers " + lsSQL);
            JSONObject loJSON = GOCASRestAPI.Request_Application(_instance, lsSQL);
            
            if ("success".equals((String) loJSON.get("result"))){
                JSONObject loTemp;
                JSONArray laTemp;
                JSONArray laPayload = (JSONArray) loJSON.get("payload");
                
                _instance.beginTrans();
                
                for (int lnCtr = 0; lnCtr <= laPayload.size()-1; lnCtr++){
                    laTemp = (JSONArray) laPayload.get(lnCtr);
                    
                    for (int lnCtr2 = 0; lnCtr2 <= laTemp.size()-1; lnCtr2++){
                        loTemp = (JSONObject) laTemp.get(lnCtr2);
                        
                        if ("success".equals((String) loTemp.get("result"))){
                            if (!saveData(_instance, (String) loTemp.get("table"), loTemp, logwrapr)){
                                _instance.rollbackTrans();
                                System.exit(1);
                            }
                        }
                    }
                }   
                
                _instance.commitTrans();
                
                logwrapr.info("Thank you.");
                System.exit(0);
            }
            
            JSONObject loTemp = (JSONObject) loJSON.get("error");
            logwrapr.info((String) loTemp.get("message") + "(" + String.valueOf(loTemp.get("code")) + ")");
        } catch (SQLException ex) {
            logwrapr.severe(ex.getMessage());
        }
        
        System.exit(1);
    }
    
    public static boolean saveData(GRiderX _instance, String fsTableNme, JSONObject _data, LogWrapper _logwrapr){       
        if (_instance == null) {
            _logwrapr.severe("Application driver is not initialized.");
            return false;
        }
        
        if (_data.containsKey("payload")){
            JSONArray laPayload = (JSONArray) _data.get("payload");
            JSONObject loJSON;
            for (int lnCtr = 0; lnCtr <= laPayload.size()-1; lnCtr++){
                loJSON = (JSONObject) laPayload.get(lnCtr);
                
                return saveData(_instance, fsTableNme, loJSON, _logwrapr);
            }
            
            //we are assuming that that table has no data at all.
            return true;
        }
        
        String lsSQL = "";
     
        for (Object keyStr : _data.keySet()) {
            if (!"result".equals(keyStr) && !"table".equals(keyStr)){
                Object keyvalue = _data.get(keyStr);
            
                lsSQL += ", " + keyStr + " = " + SQLUtil.toSQL(keyvalue);
            }
        }
        
        lsSQL = "INSERT INTO " + fsTableNme + " SET " + lsSQL.substring(2);
        
        if (_instance.executeUpdate(lsSQL) <= 0){
            _logwrapr.severe(_instance.getErrMsg() + "; " + _instance.getMessage());
            return false;
        }
        
        return true;
    }
}
