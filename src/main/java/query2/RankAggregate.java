package query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Ship;

import java.util.List;
import java.util.Map;

public class RankAggregate implements AggregateFunction<Ship, AccumulatorQuery2, OutputQuery2> {
    @Override
    public AccumulatorQuery2 createAccumulator() {
        return new AccumulatorQuery2();
    }

    @Override
    public AccumulatorQuery2 add(Ship data, AccumulatorQuery2 acc) {
        //System.out.println("==dataentity: " + data);

        String shipTimestamp = data.getTimestamp();
        String[] tokens = shipTimestamp.split(" ");
        String tripTime = tokens[1];
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

        for (Map.Entry<String, List<String>> entry : acc2.getAm().entrySet()) {
            String key = entry.getKey();
            for (String elem : entry.getValue()){
                acc1.addAM(key, elem);
            }
        }

        return acc1;
    }
}
