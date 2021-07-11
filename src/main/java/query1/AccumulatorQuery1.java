package query1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumulatorQuery1 implements Serializable {

    //ship type, list of ship IDs
    private Map<String, List<String>> countShipType;

    public AccumulatorQuery1() {
        this.countShipType = new HashMap<>();
    }


    public void addShipPerType(String type, String id){

        List<String> listOfShips = countShipType.get(type);
        //System.out.println("In Accumulator: --listOfShips= "+listOfShips);
        if (listOfShips == null){
            listOfShips = new ArrayList<>();
            listOfShips.add(id);
            countShipType.put(type, listOfShips);
        }
        else{
            if (!listOfShips.contains(id)){
                listOfShips.add(id);
                countShipType.put(type, listOfShips);
            }

        }

    }

    public Map<String, List<String>> getCountShipType() {
        return countShipType;
    }

    public void setCountShipType(Map<String, List<String>> countShipType) {
        this.countShipType = countShipType;
    }

    @Override
    public String toString() {
        return "AccumulatorQuery1{" +
                "countType=" + countShipType +
                '}';
    }


}
