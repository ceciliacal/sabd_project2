package query1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumulatorQuery1 implements Serializable {

    //ship type, list of ship IDs
    private Map<String, List<String>> countType;

    public AccumulatorQuery1() {

        this.countType = new HashMap<>();
    }


    public void add(String type, String id){

        List<String> listOfShips = countType.get(type);
        System.out.println("In Accumulator: --listOfShips= "+listOfShips);
        if (listOfShips == null){
            listOfShips = new ArrayList<>();
            listOfShips.add(id);
            countType.put(type, listOfShips);
        }
        else{
            if (!listOfShips.contains(id)){
                listOfShips.add(id);
                countType.put(type, listOfShips);
            }

        }

    }

    public Map<String, List<String>> getCountType() {
        return countType;
    }

    public void setCountType(Map<String, List<String>> countType) {
        this.countType = countType;
    }

    @Override
    public String toString() {
        return "AccumulatorQuery1{" +
                "countType=" + countType +
                '}';
    }


}
