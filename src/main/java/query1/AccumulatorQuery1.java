package query1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumulatorQuery1 implements Serializable {

    private Map<String, List<String>> countType;

    public void setCountType(Map<String, List<String>> countType) {
        this.countType = countType;
    }

    public AccumulatorQuery1() {
        this.countType = new HashMap<>();
    }


    public void add(String type, String id){

        List<String> idList = countType.get(type);
        System.out.println("In Accumulator: --idList= "+idList);
        if (idList == null){
            idList = new ArrayList<>();
            idList.add(id);
            countType.put(type, idList);
        }
        else{
            if (!idList.contains(id)){
                idList.add(id);
                countType.put(type, idList);
            }

        }

    }

    /*
    public void add (String shipType){

        Double counted = countType.get(shipType);
        if (counted==null){
            counted = 0.0;

        }
        countType.put(shipType,counted+1.0);

    }

     */


    public Map<String, List<String>> getCountType() {
        return countType;
    }




    @Override
    public String toString() {
        return "AccumulatorQuery1{" +
                "countType=" + countType +
                '}';
    }


}
