package org.rmj.car;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.gocas.service.GOCASRestAPI;
import org.rmj.replication.utility.MiscReplUtil;

/**
 *
 * @author jef
 */
public class CARRequest {
    private  static final String APPLOG = "D:/GGC_Java_Systems/temp/car.temp";
    private  static final String ERRLOG = "D:/GGC_Java_Systems/temp/car.err";
    
    public static void main(String [] args) {
        //delete files
        File loFile;
        loFile = new File(APPLOG);
        if (loFile.exists()) loFile.delete();
        loFile = new File(ERRLOG);
        if (loFile.exists()) loFile.delete();
        
        String lsProdctID = "IntegSys";
        String lsUsersIDx = "M001180003";
        
        String lsProdctxx = "gRider";
        String lsUsersxxx = "M001180003";
        
        if (args.length == 0) {
            System.err.println("Invalid parameters detected.");
            MiscReplUtil.fileWrite(ERRLOG, "Invalid parameters detected.");
        }
        
        GRider poGRiderxx = new GRider(lsProdctxx);
        poGRiderxx.loadUser(lsProdctID, lsUsersxxx);
        
        GRider poGRider = new GRider(lsProdctID);
        
        if (!poGRider.loadUser(lsProdctID, lsUsersIDx)) {
            System.err.println(lsProdctID +
                                " - " + poGRider.getErrMsg() + ";" +
                                poGRider.getMessage());
            MiscReplUtil.fileWrite(ERRLOG, lsProdctID +
                                    " - " + poGRider.getErrMsg() + ";" +
                                    poGRider.getMessage());    
            System.exit(1);
        }
        
        JSONObject loJSON = GOCASRestAPI.Request(poGRider,"",args[0]);
        
        if ("success".equalsIgnoreCase((String) loJSON.get("result"))) {
            String lsSQL = "SELECT" +
                                "  sTransNox" +
                                ", sBranchCd" +
                                ", dTransact" +
                                ", dTargetDt" +
                                ", sClientNm" + 
                                ", sDetlInfo" + 
                            " FROM Credit_Online_Application" +
                            " WHERE sTransNox = " + SQLUtil.toSQL(loJSON.get("sTransNox"));
            
            Connection loCon = poGRiderxx.getConnection();
            
            if(loCon == null){
                MiscReplUtil.fileWrite(ERRLOG, lsProdctID +
                                        " - " + "Invalid connection!");
                System.exit(1);
            }

            Statement loStmt = null;
            ResultSet loRS = null;

            try {
                System.out.println("Before Execute");

                loStmt = loCon.createStatement();
                
                loRS = loStmt.executeQuery(lsSQL);
                if(MiscUtil.RecordCount(loRS)==0){
                    JSONParser loParser = new JSONParser();
                    JSONObject loDetail = new JSONObject();
                    try {
                        loDetail = (JSONObject) loParser.parse((String) loJSON.get("sCatInfox"));
                    } catch (ParseException ex) {
                        Logger.getLogger(CARRequest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                            
                    lsSQL = "INSERT INTO Credit_Online_Application SET" +
                                "  sTransNox = " + SQLUtil.toSQL(loJSON.get("sTransNox")) +
                                ", sBranchCd = " + SQLUtil.toSQL(loJSON.get("sBranchCd")) +
                                ", dTransact = " + SQLUtil.toSQL(loJSON.get("dTransact")) +
                                ", sClientNm = " + SQLUtil.toSQL(loJSON.get("sClientNm")) +
                                ", sSourceCD = " + SQLUtil.toSQL(loJSON.get("sSourceCD")) +
                                ", sDetlInfo = " + "'" + loDetail + "'" +
                                ", nDownPaym = " + Double.parseDouble(loJSON.get("nDownPaym").toString()) +
                                ", sCreatedx = " + SQLUtil.toSQL(loJSON.get("sCreatedx")) +
                                ", dCreatedx = " + SQLUtil.toSQL(loJSON.get("dCreatedx")) +
                                ", cWithCIxx = " + SQLUtil.toSQL(loJSON.get("cWithStat")) +
                                ", cTranStat = " + SQLUtil.toSQL(loJSON.get("cTranStat")) +
                                ", cDivision = " + SQLUtil.toSQL(loJSON.get("cDivision")) +
                                ", cEvaluatr = " + SQLUtil.toSQL(loJSON.get("cEvaluatr")) +
                                ", sQMatchNo = " + SQLUtil.toSQL("") +
                                ", dModified = " + SQLUtil.toSQL(loJSON.get("dModified"));                     
                    
                    if(poGRiderxx.executeUpdate(lsSQL) == 0){
                        if(!poGRiderxx.getErrMsg().isEmpty())
                            MiscReplUtil.fileWrite(ERRLOG, lsProdctID +
                                            " - " + poGRider.getErrMsg());
                        else
                            MiscReplUtil.fileWrite(ERRLOG, lsProdctID +
                                            " - " + "No record updated");
                    }
                }else{
                    MiscReplUtil.fileWrite(ERRLOG, lsProdctID +
                                            " - " + "Already captured!");
                    System.exit(1);
                }
                                           
                System.out.println("After Execute");

            } catch (SQLException ex) {
                MiscReplUtil.fileWrite(ERRLOG, lsProdctID +
                                        " - " + ex.getMessage());
            }
            finally{
                MiscUtil.close(loRS);
                MiscUtil.close(loStmt);
            }
        }
    }
}