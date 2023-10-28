package org.rmj.gocas;

import java.io.File;
import java.sql.ResultSet;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.SQLUtil;
import org.rmj.gocas.service.GOCASRestAPI;
import org.rmj.replication.utility.MiscReplUtil;

public class GOCAS_Request {
    private static final String APPLOG = "D:/GGC_Java_Systems/temp/gocas.tmp";
    private static final String ERRLOG = "D:/GGC_Java_Systems/temp/gocas.err";
    
    public static void main(String [] args){
        //delete files
        File loFile;
        loFile = new File(APPLOG);
        if (loFile.exists()) loFile.delete();
        loFile = new File(ERRLOG);
        if (loFile.exists()) loFile.delete();
        //end: delete files
        
        String lsProdctID = "IntegSys";
        String lsUserIDxx = "M001111122";

        if (args.length == 0){
            System.err.println("Invalid parameters detected.");
            MiscReplUtil.fileWrite(ERRLOG, "Invalid parameters detected.");
            System.exit(1);
        }
        
        GRider poGRider = new GRider(lsProdctID);
        
        if (!poGRider.loadUser(lsProdctID, lsUserIDxx)){
            System.err.println(lsProdctID + 
                                " - " + poGRider.getErrMsg() + ";" + 
                                        poGRider.getMessage());
            MiscReplUtil.fileWrite(ERRLOG, lsProdctID + 
                                            " - " + poGRider.getErrMsg() + ";" + 
                                                    poGRider.getMessage());
            System.exit(1);
        }
        
        JSONObject loJSON = GOCASRestAPI.Request(poGRider, "", args[0]);
        
        if ("success".equalsIgnoreCase((String) loJSON.get("result"))){          
            String lsSQL = "SELECT" +
                                " sTransNox" +
                            " FROM Credit_Online_Application" +
                            " WHERE sTransNox = " + SQLUtil.toSQL((String) loJSON.get("sTransNox"));
            
            
            ResultSet loRS = poGRider.executeQuery(lsSQL);
            
            //save record if not exists
            //if (MiscUtil.RecordCount(loRS) <= 0){
            JSONParser loParser = new JSONParser();
            JSONObject loDetail = new JSONObject();
            try {
                loDetail = (JSONObject) loParser.parse((String) loJSON.get("sCatInfox"));   
            } catch (ParseException ex) {
                System.err.println(ex.getMessage());
                MiscReplUtil.fileWrite(ERRLOG, (String) loJSON.get("sTransNox") + 
                                                " - " + ex.getMessage());
                System.exit(1);
            }

            lsSQL = "INSERT INTO Credit_Online_Application SET" +
                        "  sTransNox = " + SQLUtil.toSQL((String) loJSON.get("sTransNox")) +
                        ", sBranchCd = " + SQLUtil.toSQL((String) loJSON.get("sBranchCd")) +
                        ", dTransact = " + SQLUtil.toSQL((String) loJSON.get("dTransact")) +
                        ", sClientNm = " + SQLUtil.toSQL((String) loJSON.get("sClientNm")) +
                        ", sGOCASNox = " + SQLUtil.toSQL((String) loJSON.get("sGOCASNox")) +
                        ", cUnitAppl = " + SQLUtil.toSQL((String) loJSON.get("cUnitAppl")) +
                        ", sSourceCD = " + SQLUtil.toSQL((String) loJSON.get("sSourceCD")) +
                        ", sDetlInfo = ''" + 
                        ", sCatInfox = " + "'" + (String) loJSON.get("sCatInfox") + "'" +
                        ", nDownPaym = " + (String) loJSON.get("nDownPaym") +
                        ", sQMatchNo = " + SQLUtil.toSQL((String) loJSON.get("sQMatchNo")) +
                        ", sCoMkrRs1 = " + SQLUtil.toSQL((String) loJSON.get("sCoMkrRs1")) +
                        ", sCoMkrRs2 = " + SQLUtil.toSQL((String) loJSON.get("sCoMkrRs2")) +
                        ", sCreatedx = " + SQLUtil.toSQL((String) loJSON.get("sCreatedx")) +
                        ", dCreatedx = " + SQLUtil.toSQL((String) loJSON.get("dCreatedx")) +
                        ", sVerified = " + SQLUtil.toSQL((String) loJSON.get("sVerified")) +
                        ", dVerified = " + SQLUtil.toSQL((String) loJSON.get("dVerified")) +
                        ", cWithCIxx = " + SQLUtil.toSQL((String) loJSON.get("cWithCIxx")) +
                        ", cTranStat = " + SQLUtil.toSQL((String) loJSON.get("cTranStat")) +
                        ", cDivision = " + SQLUtil.toSQL((String) loJSON.get("cDivision")) + 
                    " ON DUPLICATE KEY UPDATE" + 
                        "  sBranchCd = " + SQLUtil.toSQL((String) loJSON.get("sBranchCd")) +
                        ", dTransact = " + SQLUtil.toSQL((String) loJSON.get("dTransact")) +
                        ", sClientNm = " + SQLUtil.toSQL((String) loJSON.get("sClientNm")) +
                        ", sGOCASNox = " + SQLUtil.toSQL((String) loJSON.get("sGOCASNox")) +
                        ", cUnitAppl = " + SQLUtil.toSQL((String) loJSON.get("cUnitAppl")) +
                        ", sSourceCD = " + SQLUtil.toSQL((String) loJSON.get("sSourceCD")) +
                        ", sDetlInfo = ''" + 
                        ", sCatInfox = " + "'" + (String) loJSON.get("sCatInfox") + "'" +
                        ", nDownPaym = " + (String) loJSON.get("nDownPaym") +
                        ", sQMatchNo = " + SQLUtil.toSQL((String) loJSON.get("sQMatchNo")) +
                        ", sCoMkrRs1 = " + SQLUtil.toSQL((String) loJSON.get("sCoMkrRs1")) +
                        ", sCoMkrRs2 = " + SQLUtil.toSQL((String) loJSON.get("sCoMkrRs2")) +
                        ", sCreatedx = " + SQLUtil.toSQL((String) loJSON.get("sCreatedx")) +
                        ", dCreatedx = " + SQLUtil.toSQL((String) loJSON.get("dCreatedx")) +
                        ", sVerified = " + SQLUtil.toSQL((String) loJSON.get("sVerified")) +
                        ", dVerified = " + SQLUtil.toSQL((String) loJSON.get("dVerified")) +
                        ", cWithCIxx = " + SQLUtil.toSQL((String) loJSON.get("cWithCIxx")) +
                        ", cTranStat = " + SQLUtil.toSQL((String) loJSON.get("cTranStat")) +
                        ", cDivision = " + SQLUtil.toSQL((String) loJSON.get("cDivision"));

            long lnRow = poGRider.executeUpdate(lsSQL);

            if (lnRow != 0){
                System.out.println((String) loJSON.get("sTransNox") + " downloaded successfuly.");
                MiscReplUtil.fileWrite(APPLOG, (String) loJSON.get("sTransNox") + " downloaded successfuly.");
            }else{
                System.err.println((String) loJSON.get("sTransNox") + 
                                    " - " + poGRider.getErrMsg() + ";" + 
                                            poGRider.getMessage());
                MiscReplUtil.fileWrite(ERRLOG, (String) loJSON.get("sTransNox") + 
                                                " - " + poGRider.getErrMsg() + ";" + 
                                                        poGRider.getMessage());
                System.exit(1);
            }                    
            //} else {
            //    System.out.println((String) loJSON.get("sTransNox") + " already exists.");
            //    MiscReplUtil.fileWrite(APPLOG, (String) loJSON.get("sTransNox") + " already exists.");
            //}
        } else{
            loJSON = (JSONObject) loJSON.get("error");
            
            System.err.println((String) loJSON.get("sTransNox") + " - " + (String) loJSON.get("message"));
            MiscReplUtil.fileWrite(ERRLOG, (String) loJSON.get("sTransNox") + " - " + (String) loJSON.get("message"));
            System.exit(1);
        }
        
        //return success
        System.exit(0);
    }
}
