package query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Ship;
import java.util.*;

public class AverageAggregate implements AggregateFunction<Ship, AccumulatorQuery1, OutputQuery1> {


    @Override
    public AccumulatorQuery1 createAccumulator() {

        return new AccumulatorQuery1();
    }

    @Override
    public AccumulatorQuery1 add(Ship data, AccumulatorQuery1 acc) {
        System.out.println("==dataentity: "+data);
        System.out.println("---add");

        //creo id univoco per una nave in un determinato giorno
        //concatenando id nave + data giornanliera, così nave viene contata 1 nello stesso giorno su una cella
        //ma in un giorno diverso viene ricontata
        String shipId = data.getShipId();
        String tripDay = data.getTripDay();
        System.out.println("GIORNO DI MERDA: "+tripDay);
        String dailyTrip = shipId+tripDay;

        acc.add(data.getShipType(), dailyTrip);

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
        //trasferisco elem di un accumulator su un altro
        System.out.println("---merge");
        acc2.getCountShipType().forEach((k, v) -> acc1.getCountShipType().merge(k, v, (v1, v2) -> {
            Set<String> set = new TreeSet<>(v1);
            set.addAll(v2);
            return  new ArrayList<>(set);
        }));
        return acc1;
    }
}
