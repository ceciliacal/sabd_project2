package query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Ship;

public class RankAggregate implements AggregateFunction<Ship, AccumulatorQuery2, OutputQuery2> {
    @Override
    public AccumulatorQuery2 createAccumulator() {
        return new AccumulatorQuery2();
    }

    @Override
    public AccumulatorQuery2 add(Ship data, AccumulatorQuery2 acc) {
        //System.out.println("==dataentity: " + data);
        System.out.println("---add");

        String shipTimestamp = data.getTimestamp();
        String[] tokens = shipTimestamp.split(" ");
        String tripTime = tokens[1];
        System.out.println("GIORNO DI MERDA: "+tokens[0]);
        String amOrPm = Query2.checkDate(tripTime);

        if (amOrPm.equals("am")) {
            acc.addAM(data.getCell(), data.getShipId());
        } else {
            acc.addPM(data.getCell(), data.getShipId());
        }

        return acc;
    }

    @Override
    public OutputQuery2 getResult(AccumulatorQuery2 acc) {
        System.out.println("---result");
        acc.getAm().forEach((k, v) -> System.out.println("AM k: " + k + " v: " + v));
        acc.getPm().forEach((k, v) -> System.out.println("PM k: " + k + " v: " + v));
        return new OutputQuery2(acc.getAm(), acc.getPm());

    }

    @Override
    public AccumulatorQuery2 merge(AccumulatorQuery2 acc1, AccumulatorQuery2 acc2) {
        return null;
    }
}
