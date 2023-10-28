package org.rmj.dcp;

import org.rmj.appdriver.agent.GRiderX;
import org.rmj.replication.utility.LogWrapper;

public class Import2Server {
    public static void main(String [] args){
        LogWrapper logwrapr = new LogWrapper("DCP.ImportFile", "dcp.log");
        
//        if (args.length <= 1){
//            logwrapr.severe("Invalid parameter detected.");
//            System.exit(1);
//        }
//        
//        if (args[0].isEmpty()){
//            logwrapr.severe("Invalid parameter detected.");
//            System.exit(1);
//        }
        
        GRiderX poGRider = new GRiderX("IntegSys");
        
        //args[1]
        if (!poGRider.logUser("IntegSys", "M001111122")){
            logwrapr.severe(poGRider.getMessage() + ", " + poGRider.getErrMsg());
            System.exit(1);
        }
        
        if(!poGRider.getErrMsg().isEmpty()){
            logwrapr.severe(poGRider.getErrMsg());
            logwrapr.severe("GRiderX has error...");
            System.exit(1);
        }
        
        poGRider.setOnline(false);
        //args[0]
        if (DCPUtil.ImportToServer(poGRider, "M04821000583", logwrapr)){
            logwrapr.info("File imported successfully. - " + args[0]);
            System.exit(0);
        } else
            System.exit(1);
    }
}
