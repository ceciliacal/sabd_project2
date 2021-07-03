package query1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class AccumulatorQuery1 implements Serializable {

    private Tuple2<String,Double> tupla;

    public AccumulatorQuery1(Tuple2<String, Double> tupla) {
        this.tupla = tupla;
    }

    public AccumulatorQuery1() {
    }


    public Tuple2<String, Double> getTupla() {
        return tupla;
    }

    public void setTupla(Tuple2<String, Double> tupla) {
        this.tupla = tupla;
    }
}
