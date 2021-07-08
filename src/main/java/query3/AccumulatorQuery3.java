package query3;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AccumulatorQuery3 implements Serializable {

    //k: timestamp, v: <lat, lon>
    private Map<Date, Tuple2<Double, Double>> positionTimestamp;

    public AccumulatorQuery3() {
        this.positionTimestamp = new HashMap<>();
    }

    public void addNewPosition(Date ts, Double lat, Double lon){
        //qui non controllo se key c'è già perché sto lavorando solo con un certo tipo
        // di tripId ed poiché nell'hashmap la chiave è il ts, dovrebbero essere
        //tutti diversi e differire per data e per l'orario
        Tuple2<Double, Double> coordinates = new Tuple2<>(lat, lon);
        positionTimestamp.put(ts, coordinates);

    }

    public Map<Date, Tuple2<Double, Double>> getPositionTimestamp() {
        return positionTimestamp;
    }

    public void setPositionTimestamp(Map<Date, Tuple2<Double, Double>> positionTimestamp) {
        this.positionTimestamp = positionTimestamp;
    }

}