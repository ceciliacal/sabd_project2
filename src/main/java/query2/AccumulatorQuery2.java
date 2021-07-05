package query2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumulatorQuery2 {

    //k: cella, v: lista id nave
    private Map<String, List<String>> am;
    private Map<String, List<String>> pm;

    public AccumulatorQuery2(){
        this.am = new HashMap<>();
        this.pm = new HashMap<>();
    }

    public void addAM(String cell, String shipId) {

        List<String> listOfShips = am.get(cell);
        System.out.println("In Accumulator : --listOfShips AM= "+listOfShips);
        if (listOfShips==null){
            listOfShips = new ArrayList<>();
            listOfShips.add(shipId);
            am.put(cell, listOfShips);
        }
        else{
            if (!listOfShips.contains(shipId)){
                listOfShips.add(shipId);
                am.put(cell, listOfShips);
            }
        }
    }

    public void addPM(String cell, String shipId) {

        List<String> listOfShips = pm.get(cell);
        System.out.println("In Accumulator : --listOfShips PM= "+listOfShips);
        if (listOfShips==null){
            listOfShips = new ArrayList<>();
            listOfShips.add(shipId);
            pm.put(cell, listOfShips);
        }
        else{
            if (!listOfShips.contains(shipId)){
                listOfShips.add(shipId);
                pm.put(cell, listOfShips);
            }
        }
    }

    @Override
    public String toString() {
        return "AccumulatorQuery2{" +
                "am=" + am +
                ", pm=" + pm +
                '}';
    }

    public Map<String, List<String>> getAm() {
        return am;
    }

    public void setAm(Map<String, List<String>> am) {
        this.am = am;
    }

    public Map<String, List<String>> getPm() {
        return pm;
    }

    public void setPm(Map<String, List<String>> pm) {
        this.pm = pm;
    }


}
