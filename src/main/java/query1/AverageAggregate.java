package query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Ship;
import java.util.*;

public class AverageAggregate implements AggregateFunction<Ship, AccumulatorQuery1, OutputQuery1> {


    @Override
    public AccumulatorQuery1 createAccumulator() {

        //istant now
        //get result faccio diff
        return new AccumulatorQuery1();
    }

    @Override
    public AccumulatorQuery1 add(Ship data, AccumulatorQuery1 acc) {
        //System.out.println("==dataentity: "+data);
        //System.out.println("---add");

        //creo id univoco per una nave in un determinato giorno
        //concatenando id nave + data giornanliera, così nave viene contata 1 nello stesso giorno su una cella
        //ma in un giorno diverso viene ricontata
        String shipId = data.getShipId();
        String tripDay = data.getTripDay();
        String dailyTrip = shipId+tripDay;

        acc.addShipPerType(data.getShipType(), dailyTrip);

        return acc;
    }

    @Override
    public OutputQuery1 getResult(AccumulatorQuery1 acc) {
        System.out.println("---result");
        //qui sto restituendo il totale di ogni value, cioè di ogni volta che la finestra ha in teoria processato
        acc.getCountShipType().forEach((k, v) -> System.out.println("k: "+k+" v: "+v));
        return new OutputQuery1(acc.getCountShipType());
    }

    @Override
    public AccumulatorQuery1 merge(AccumulatorQuery1 acc1, AccumulatorQuery1 acc2) {
        //trasferisco elem di un accumulator su un altro sommandoli
        System.out.println("---merge");
        for (Map.Entry<String, List<String>> entry : acc2.getCountShipType().entrySet()) {
            String key = entry.getKey();
            for (String elem : entry.getValue()){
                acc1.addShipPerType(key, elem);
            }
        }

        return acc1;
        }

}

