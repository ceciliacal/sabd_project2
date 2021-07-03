package query1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumulatorQuery1 implements Serializable {

    private Map<String, Double> countType;

    public AccumulatorQuery1() {
        this.countType = new HashMap<>();
    }


    public void add (String shipType){

        Double counted = countType.get(shipType);
        if (counted==null){
            counted = 0.0;

        }
        countType.put(shipType,counted+1.0);

    }


    public Map<String, Double> getCountType() {
        return countType;
    }

    public void setCountType(Map<String, Double> countType) {
        this.countType = countType;
    }



    @Override
    public String toString() {
        return "AccumulatorQuery1{" +
                "countType=" + countType +
                '}';
    }


}
