
import org.rmj.car.GOCAS_Save;
import org.rmj.gocas.GOCAS_Request;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Mac
 */
public class testGOCASSave {
    public static void main(String [] args){
        String [] argx = new String [2];
        
        argx[0] = "M001111122";
        argx[1] = "0";
        
        GOCAS_Save.main(argx);
    }
}
