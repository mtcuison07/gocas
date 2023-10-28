
import org.rmj.dcp.ImportFile;



public class testDCPImport { 
    public static void main(String [] args){
        String [] argx = new String [2];

        argx[0] = "D:\\dcp\\M00121000036-mob.txt";
        argx[0] = "M00121000036";
        argx[1] = "M001111122";
        
        ImportFile.main(argx);
    }
}
