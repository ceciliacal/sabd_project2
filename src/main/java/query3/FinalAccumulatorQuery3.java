package query3;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FinalAccumulatorQuery3 implements Serializable {

    //k: distance v: tripId
    private TreeMap<Double, String> distanceTripId;

    public FinalAccumulatorQuery3(){
        this.distanceTripId = new TreeMap<>();
    }

    public void addToCalculateRank(Double distance, String tripId){
        distanceTripId.put(distance, tripId);
    }

    public TreeMap<Double, String> getDistanceTripId() {
        return distanceTripId;
    }

    public void setDistanceTripId(TreeMap<Double, String> distanceTripId) {
        this.distanceTripId = distanceTripId;
    }
}
